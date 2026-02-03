import React, { useRef, useMemo } from 'react';
import { Canvas, useFrame } from '@react-three/fiber';
import { Float, Text, PerspectiveCamera, OrbitControls } from '@react-three/drei';
import * as THREE from 'three';
import { motion } from 'framer-motion';

const isWebGLAvailable = () => {
    try {
        const canvas = document.createElement('canvas');
        return !!(window.WebGLRenderingContext && (canvas.getContext('webgl') || canvas.getContext('experimental-webgl')));
    } catch (e) {
        return false;
    }
};

const CSSFunnelFallback = ({ data }) => {
    return (
        <div className="flex flex-col items-center justify-center h-full w-full py-8" style={{ perspective: '1000px' }}>
            {data.map((stage, idx) => {
                const width = 100 - (idx * 20);
                return (
                    <motion.div
                        key={stage.name}
                        className="relative my-3 flex items-center justify-between px-6 overflow-hidden"
                        initial={{ opacity: 0, x: -50, rotateX: 45 }}
                        animate={{ opacity: 1, x: 0, rotateX: 25 }}
                        transition={{ delay: idx * 0.1, type: 'spring' }}
                        style={{
                            width: `${width}%`,
                            height: '60px',
                            background: `linear-gradient(90deg, ${stage.color}88, ${stage.color}22)`,
                            borderLeft: `5px solid ${stage.color}`,
                            borderRadius: '12px',
                            boxShadow: `0 15px 35px -10px ${stage.color}66`,
                            backdropFilter: 'blur(10px)',
                            transformStyle: 'preserve-3d'
                        }}
                    >
                        <span className="font-bold text-sm tracking-wider uppercase">{stage.name}</span>
                        <span className="font-mono text-xl font-black">{Math.round(stage.value).toLocaleString()}</span>

                        {/* 3D Depth decoration */}
                        <div style={{
                            position: 'absolute', bottom: '-5px', left: '0', width: '100%', height: '5px',
                            background: stage.color, opacity: 0.4, transform: 'rotateX(-90deg)', transformOrigin: 'bottom'
                        }}></div>
                    </motion.div>
                );
            })}
        </div>
    );
};

const FunnelSegment = ({ position, radiusTop, radiusBottom, height, color, label, value }) => {
    const meshRef = useRef();

    // Create a custom geometry for the cone segment
    const geometry = useMemo(() => {
        return new THREE.CylinderGeometry(radiusTop, radiusBottom, height, 32, 1, false);
    }, [radiusTop, radiusBottom, height]);

    return (
        <group position={position}>
            <mesh ref={meshRef} geometry={geometry}>
                <meshPhysicalMaterial
                    color={color}
                    transmission={0.3}
                    thickness={0.5}
                    roughness={0.2}
                    metalness={0.8}
                    emissive={color}
                    emissiveIntensity={0.2}
                    transparent
                    opacity={0.8}
                />
            </mesh>

            {/* Label and Value */}
            <Float speed={2} rotationIntensity={0.5} floatIntensity={0.5}>
                <Text
                    position={[radiusTop + 2.5, 0, 0]}
                    fontSize={0.4}
                    color="white"
                    anchorX="left"
                    font="https://fonts.gstatic.com/s/outfit/v11/QGYsz_XF6RW27i6TXC1v.woff"
                >
                    {`${label}: ${Math.round(value).toLocaleString()}`}
                </Text>
            </Float>

            {/* Internal GLOW */}
            <mesh geometry={geometry} scale={[0.95, 1.01, 0.95]}>
                <meshBasicMaterial color={color} transparent opacity={0.15} side={THREE.BackSide} />
            </mesh>
        </group>
    );
};

const FunnelScene = ({ data }) => {
    const groupRef = useRef();

    // Rotate the whole funnel slowly
    useFrame((state) => {
        if (groupRef.current) {
            groupRef.current.rotation.y = Math.sin(state.clock.elapsedTime * 0.5) * 0.2;
        }
    });

    // Calculate funnel dimensions based on data
    // data: [{ name, value, color }]
    const segments = useMemo(() => {
        if (!data || data.length === 0) return [];

        const heightPerSegment = 2.5;
        const gap = 0.4;
        const maxVal = Math.max(...data.map(d => d.value)) || 1;
        const baseRadius = 2.5;

        return data.map((d, i) => {
            const radiusTop = (d.value / maxVal) * baseRadius + 1;
            // If there's a next segment, radiusBottom should match its radiusTop for a smooth look, 
            // or we can just scale it.
            const nextVal = data[i + 1] ? data[i + 1].value : d.value * 0.7;
            const radiusBottom = (nextVal / maxVal) * baseRadius + 1;

            return {
                id: d.name,
                position: [0, (data.length - 1 - i) * (heightPerSegment + gap) - (data.length * heightPerSegment) / 2, 0],
                radiusTop,
                radiusBottom,
                height: heightPerSegment,
                color: d.color,
                label: d.name,
                value: d.value
            };
        });
    }, [data]);

    return (
        <group ref={groupRef}>
            {segments.map(seg => (
                <FunnelSegment key={seg.id} {...seg} />
            ))}
            <ambientLight intensity={0.5} />
            <pointLight position={[10, 10, 10]} intensity={1.5} />
            <spotLight position={[-10, 20, 10]} angle={0.15} penumbra={1} intensity={2} />
        </group>
    );
};

const ThreeDFunnel = ({ data }) => {
    const hasWebGL = useMemo(() => isWebGLAvailable(), []);

    return (
        <div className="h-[450px] w-full" style={{ height: '450px', width: '100%' }}>
            {hasWebGL ? (
                <Canvas shadows dpr={[1, 2]}>
                    <PerspectiveCamera makeDefault position={[0, 0, 12]} fov={40} />
                    <OrbitControls enableZoom={false} enablePan={false} maxPolarAngle={Math.PI / 1.8} minPolarAngle={Math.PI / 2.2} />
                    <FunnelScene data={data} />
                </Canvas>
            ) : (
                <CSSFunnelFallback data={data} />
            )}
        </div>
    );
};

export default ThreeDFunnel;
