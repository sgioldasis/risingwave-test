import React from 'react';
import { motion } from 'framer-motion';

const PureCSSFunnel = ({ data }) => {
    return (
        <div className="flex flex-col items-start justify-center h-full w-full py-4">
            {data.map((stage, idx) => {
                const width = 95 - (idx * 15); // Slightly smaller to prevent spilling
                return (
                    <motion.div
                        key={stage.name}
                        className="relative my-3 flex items-center justify-between px-6 overflow-hidden"
                        initial={{ opacity: 0, x: -20 }}
                        animate={{ opacity: 1, x: 0 }}
                        whileHover={{ scale: 1.02, backgroundColor: 'rgba(255,255,255,0.05)' }}
                        style={{
                            width: `${width}%`,
                            height: '70px',
                            background: `linear-gradient(90deg, ${stage.color}aa, ${stage.color}11)`,
                            borderLeft: `5px solid ${stage.color}`,
                            borderRadius: '12px',
                            boxShadow: `0 8px 24px -10px rgba(0,0,0,0.3)`,
                            backdropFilter: 'blur(12px)',
                            cursor: 'pointer'
                        }}
                    >
                        <div className="flex flex-col gap-1">
                            <span className="font-bold text-xs tracking-widest uppercase opacity-60" style={{ fontFamily: "'Outfit', sans-serif", fontWeight: 800, marginLeft: '8px' }}>{stage.name}</span>
                            <div className="flex items-baseline gap-3" style={{ marginLeft: '8px' }}>
                                <span className="font-mono text-3xl font-black leading-none text-white/90">
                                    {Math.round(stage.value).toLocaleString()}
                                </span>
                                <span className="font-mono text-sm font-bold text-white/40">
                                    &nbsp;({idx === 0 ? '100%' : `${(stage.value / data[0].value * 100).toFixed(1)}%`})
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
