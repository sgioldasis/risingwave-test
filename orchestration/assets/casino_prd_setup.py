"""Casino production prerequisite assets: fetch, compile, and upload proto schemas."""
import shutil
import subprocess
from pathlib import Path

import boto3
import httpx
from dagster import AssetExecutionContext, MetadataValue, asset

PROTO_DIR = Path(__file__).parent.parent.parent / "proto"

APICURIO_BASE = "http://staging-schema-registry.kaizengaming.net/apis/registry/v2/groups/bigdata/artifacts"
CASINO_ARTIFACT = "casinoroundinfo"
BETS_ARTIFACT = "betinfo"

MINIO_ENDPOINT = "http://minio-0:9301"
MINIO_BUCKET = "hummock001"
MINIO_ACCESS_KEY = "hummockadmin"
MINIO_SECRET_KEY = "hummockadmin"


@asset(group_name="casino_prd_setup", description="Fetch .proto files from Apicurio schema registry (native v2)")
def casino_prd_proto_fetch(context: AssetExecutionContext):
    """Fetch casino and bets .proto sources from Apicurio native v2 endpoint."""
    PROTO_DIR.mkdir(parents=True, exist_ok=True)

    fetched = []
    for artifact, filename in [
        (CASINO_ARTIFACT, "casinoroundinfodto.proto"),
        (BETS_ARTIFACT, "betinfo.proto"),
    ]:
        dest = PROTO_DIR / filename
        url = f"{APICURIO_BASE}/{artifact}"
        context.log.info(f"Fetching {url} → {dest}")
        response = httpx.get(url, headers={"Accept": "text/plain"}, timeout=30, follow_redirects=True)
        response.raise_for_status()
        dest.write_bytes(response.content)
        context.log.info(f"Saved {dest} ({dest.stat().st_size} bytes)")
        fetched.append(str(dest))

    return {"fetched_files": MetadataValue.json(fetched)}


@asset(
    group_name="casino_prd_setup",
    deps=[casino_prd_proto_fetch],
    description="Compile .proto files to binary FileDescriptorSet using protoc",
)
def casino_prd_proto_compile(context: AssetExecutionContext):
    """Compile .proto sources to .pb/.desc FileDescriptorSets for RisingWave."""
    compiled = []

    for proto_file, out_file, extra_args in [
        (
            "casinoroundinfodto.proto",
            "casinoroundinfodto.pb",
            [],
        ),
        (
            "betinfo.proto",
            "betinfo.desc",
            ["--proto_path=/opt/homebrew/include", f"--proto_path={PROTO_DIR}"],
        ),
    ]:
        src = PROTO_DIR / proto_file
        dest = PROTO_DIR / out_file

        if not src.exists():
            raise FileNotFoundError(f"Proto source not found: {src}. Run casino_prd_proto_fetch first.")

        if shutil.which("protoc") is None:
            if dest.exists():
                context.log.warning(f"protoc not on PATH — using existing {dest}")
                compiled.append(str(dest))
                continue
            raise RuntimeError(
                f"protoc not found and {dest} does not exist. "
                "Install protoc or pre-build the descriptor and place it in proto/."
            )

        cmd = [
            "protoc",
            "--include_imports",
            f"--descriptor_set_out={dest}",
            f"--proto_path={PROTO_DIR}",
            *extra_args,
            str(src),
        ]
        context.log.info(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"protoc failed for {proto_file}:\n{result.stderr}")
        context.log.info(f"Compiled {dest} ({dest.stat().st_size} bytes)")
        compiled.append(str(dest))

    return {"compiled_files": MetadataValue.json(compiled)}


@asset(
    group_name="casino_prd_setup",
    deps=[casino_prd_proto_compile],
    description="Upload compiled proto descriptors to MinIO at s3://hummock001/proto/",
)
def casino_prd_proto_upload(context: AssetExecutionContext):
    """Upload .pb/.desc files to MinIO so RisingWave can fetch them at CREATE TABLE time."""
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )

    uploaded = []
    for filename in ["casinoroundinfodto.pb", "betinfo.desc"]:
        local_path = PROTO_DIR / filename
        if not local_path.exists():
            raise FileNotFoundError(f"{local_path} not found. Run casino_prd_proto_compile first.")

        s3_key = f"proto/{filename}"
        context.log.info(f"Uploading {local_path} → s3://{MINIO_BUCKET}/{s3_key}")
        s3.upload_file(str(local_path), MINIO_BUCKET, s3_key)
        uri = f"s3://{MINIO_BUCKET}/{s3_key}"
        context.log.info(f"Uploaded {uri}")
        uploaded.append(uri)

    return {"uploaded_uris": MetadataValue.json(uploaded)}
