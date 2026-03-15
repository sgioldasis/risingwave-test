#!/bin/bash
# Install PostgreSQL JDBC driver in StarRocks containers

set -e

DRIVER_URL="https://jdbc.postgresql.org/download/postgresql-42.7.3.jar"
DRIVER_NAME="postgresql-42.7.3.jar"

echo "Installing PostgreSQL JDBC driver in StarRocks..."

# Download driver if not exists
if [ ! -f "starrocks/${DRIVER_NAME}" ]; then
    echo "Downloading PostgreSQL JDBC driver..."
    curl -L -o "starrocks/${DRIVER_NAME}" "${DRIVER_URL}"
fi

# Copy to FE container
echo "Copying driver to starrocks-fe..."
docker cp "starrocks/${DRIVER_NAME}" starrocks-fe:/opt/starrocks/fe/lib/

# Copy to CN container
echo "Copying driver to starrocks-cn..."
docker cp "starrocks/${DRIVER_NAME}" starrocks-cn:/opt/starrocks/be/lib/

echo "PostgreSQL JDBC driver installed successfully!"
echo ""
echo "You can now create the RisingWave catalog:"
echo "  docker run --rm --network risingwave-test_iceberg_net mysql:8 \\"
echo "    mysql -hstarrocks-fe -P9030 -uroot -e \\"
echo "    \"CREATE EXTERNAL CATALOG risingwave_catalog \\"
echo "     PROPERTIES (\\"
echo "       \\\"type\\\" = \\\"jdbc\\\", \\"
echo "       \\\"user\\\" = \\\"root\\\", \\"
echo "       \\\"password\\\" = \\\"root\\\", \\"
echo "       \\\"jdbc_uri\\\" = \\\"jdbc:postgresql://frontend-node-0:4566/dev\\\", \\"
echo "       \\\"driver_class\\\" = \\\"org.postgresql.Driver\\\", \\"
echo "       \\\"driver_url\\\" = \\\"file:///opt/starrocks/be/lib/${DRIVER_NAME}\\\" \\"
echo "     );\\""
