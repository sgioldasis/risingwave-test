import React from 'react';
import { motion } from 'framer-motion';

const PureCSSFunnel = ({ data }) => {
    return (
        <div className="flex flex-col items-start justify-center h-full w-full py-1">
            {data.map((stage, idx) => {
                const width = 95 - (idx * 15); // Slightly smaller to prevent spilling
                return (
                    <motion.div
                        key={stage.name}
                        className="relative my-1 flex items-center justify-between px-4 overflow-hidden"
                        initial={{ opacity: 0, x: -20 }}
                        animate={{ opacity: 1, x: 0 }}
                        whileHover={{ scale: 1.02, backgroundColor: 'rgba(255,255,255,0.05)' }}
                        style={{
                            width: `${width}%`,
                            height: '48px',
                            background: `linear-gradient(90deg, ${stage.color}aa, ${stage.color}11)`,
                            borderLeft: `4px solid ${stage.color}`,
                            borderRadius: '10px',
                            boxShadow: `0 4px 12px -4px rgba(0,0,0,0.3)`,
                            backdropFilter: 'blur(12px)',
                            cursor: 'pointer'
                        }}
                    >
                        <div className="flex flex-col gap-0">
                            <span className="font-bold text-[10px] tracking-wider uppercase opacity-60" style={{ fontFamily: "'Outfit', sans-serif", fontWeight: 700, marginLeft: '4px' }}>{stage.name}</span>
                            <div className="flex items-baseline gap-2" style={{ marginLeft: '4px' }}>
                                <span className="font-mono text-xl font-bold leading-none text-white/90">
                                    {Math.round(stage.value).toLocaleString()}
                                </span>
                                <span className="font-mono text-xs font-bold text-white/40">
                                    ({idx === 0 ? '100%' : `${(stage.value / data[0].value * 100).toFixed(1)}%`})
                                </span>
                            </div>
                        </div>

                        {/* Glossy top highlight */}
                        <div className="absolute top-0 left-0 right-0 h-[1px] bg-gradient-to-r from-transparent via-white/20 to-transparent pointer-events-none" />

                        {/* Soft Glow effect */}
                        <div className="absolute inset-0 bg-gradient-to-tr from-white/5 to-transparent pointer-events-none" />
                    </motion.div>
                );
            })}
        </div>
    );
};

export default PureCSSFunnel;
