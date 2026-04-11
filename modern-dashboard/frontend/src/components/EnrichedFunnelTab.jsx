import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { Sparkles, TrendingUp, TrendingDown, Activity, BarChart3 } from 'lucide-react';

const EnrichedFunnelTab = () => {
    const [data, setData] = useState([]);
    const [health, setHealth] = useState({});
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    const fetchEnrichedData = async () => {
        try {
            const [dataRes, healthRes] = await Promise.all([
                fetch('/api/funnel/enriched?limit=20'),
                fetch('/api/funnel/health')
            ]);
            
            const dataJson = await dataRes.json();
            const healthJson = await healthRes.json();
            
            if (dataJson.error) {
                setError(dataJson.error);
            } else {
                setData(dataJson.data || []);
                setError(null);
            }
            
            if (healthJson.health_summary) {
                setHealth(healthJson.health_summary);
            }
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchEnrichedData();
        const interval = setInterval(fetchEnrichedData, 5000);
        return () => clearInterval(interval);
    }, []);

    const getCategoryColor = (category) => {
        switch (category) {
            case 'excellent': return '#00cc96';
            case 'good': return '#6c8cff';
            case 'average': return '#f59e0b';
            case 'needs_improvement': return '#ef4444';
            default: return '#9ca3af';
        }
    };

    const getHealthColor = (health) => {
        switch (health) {
            case 'strong': return '#00cc96';
            case 'moderate': return '#f59e0b';
            case 'weak': return '#ef4444';
            default: return '#9ca3af';
        }
    };

    if (loading) {
        return (
            <div className="glass-card" style={{ padding: '2rem', textAlign: 'center' }}>
                <Activity size={32} className="animate-pulse" style={{ color: '#636efa' }} />
                <p style={{ marginTop: '1rem', color: '#9ca3af' }}>Loading enriched funnel data...</p>
            </div>
        );
    }

    if (error) {
        return (
            <div className="glass-card" style={{ padding: '2rem', textAlign: 'center' }}>
                <div style={{ color: '#ef4444', marginBottom: '1rem' }}>
                    ⚠️ Error loading enriched data
                </div>
                <p style={{ color: '#9ca3af', fontSize: '0.875rem' }}>{error}</p>
                <p style={{ color: '#6b7280', fontSize: '0.75rem', marginTop: '1rem' }}>
                    Make sure Python UDFs are enabled and funnel_enriched MV exists
                </p>
            </div>
        );
    }

    return (
        <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            className="space-y-4"
        >
            {/* Header */}
            <div className="glass-card" style={{ padding: '1.5rem' }}>
                <div className="flex items-center gap-3 mb-2">
                    <Sparkles size={24} style={{ color: '#636efa' }} />
                    <h2 className="text-xl font-semibold">UDF-Enriched Funnel Analysis</h2>
                </div>
                <p className="text-sm text-gray-400">
                    Real-time funnel metrics enhanced with Python UDFs for categorization and scoring
                </p>
            </div>

            {/* Health Summary */}
            {Object.keys(health).length > 0 && (
                <div className="glass-card" style={{ padding: '1.5rem' }}>
                    <h3 className="text-lg font-semibold mb-4 flex items-center gap-2">
                        <BarChart3 size={20} />
                        Funnel Health (Last 5 Minutes)
                    </h3>
                    <div style={{
                        display: 'grid',
                        gridTemplateColumns: 'repeat(3, 1fr)',
                        gap: '0.75rem'
                    }}>
                        {['weak', 'moderate', 'strong'].map((key) => {
                            const value = health[key] || { count: 0, avg_score: 0 };
                            const healthConfig = {
                                strong: { icon: TrendingUp, label: 'Strong', color: '#10b981' },
                                moderate: { icon: Activity, label: 'Moderate', color: '#f59e0b' },
                                weak: { icon: TrendingDown, label: 'Weak', color: '#ef4444' }
                            };
                            const config = healthConfig[key];
                            const Icon = config.icon;
                            
                            return (
                                <motion.div
                                    key={key}
                                    initial={{ opacity: 0, y: 10 }}
                                    animate={{ opacity: 1, y: 0 }}
                                    whileHover={{ y: -2 }}
                                    style={{
                                        background: '#1a1d29',
                                        borderRadius: '6px',
                                        position: 'relative',
                                        overflow: 'hidden'
                                    }}
                                >
                                    {/* Top colored border */}
                                    <div
                                        style={{
                                            position: 'absolute',
                                            top: 0,
                                            left: 0,
                                            right: 0,
                                            height: '3px',
                                            background: config.color
                                        }}
                                    />
                                    
                                    <div style={{ padding: '0.875rem', paddingTop: '1rem' }}>
                                        {/* Icon */}
                                        <div style={{ marginBottom: '0.5rem' }}>
                                            <Icon size={18} style={{ color: config.color }} />
                                        </div>
                                        
                                        {/* Label */}
                                        <div
                                            style={{
                                                fontSize: '0.875rem',
                                                fontWeight: 500,
                                                textTransform: 'uppercase',
                                                letterSpacing: '0.05em',
                                                marginBottom: '0.5rem',
                                                color: 'rgba(255,255,255,0.5)'
                                            }}
                                        >
                                            {config.label}
                                        </div>
                                        
                                        {/* Count */}
                                        <div
                                            style={{
                                                fontSize: '2rem',
                                                fontWeight: 400,
                                                color: config.color
                                            }}
                                        >
                                            {value.count}
                                        </div>
                                        
                                        {/* Avg Score */}
                                        <div
                                            style={{
                                                fontSize: '0.875rem',
                                                marginTop: '0.25rem',
                                                color: 'rgba(255,255,255,0.4)'
                                            }}
                                        >
                                            Avg Score: {(value.avg_score * 100).toFixed(1)}%
                                        </div>
                                    </div>
                                </motion.div>
                            );
                        })}
                    </div>
                </div>
            )}

            {/* Data Table */}
            <div className="glass-card" style={{ padding: '1.5rem', overflowX: 'auto' }}>
                <h3 className="text-lg font-semibold mb-4">Recent Funnel Windows</h3>
                {data.length === 0 ? (
                    <p className="text-gray-400 text-center py-8">No enriched data available</p>
                ) : (
                    <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                        <thead>
                            <tr style={{ borderBottom: '1px solid rgba(255, 255, 255, 0.1)' }}>
                                <th style={{ padding: '0.75rem 1rem', textAlign: 'left', fontSize: '1rem', fontWeight: 600 }}>Time</th>
                                <th style={{ padding: '0.75rem 1rem', textAlign: 'center', fontSize: '1rem', fontWeight: 600 }}>Viewers</th>
                                <th style={{ padding: '0.75rem 1rem', textAlign: 'center', fontSize: '1rem', fontWeight: 600 }}>Carters</th>
                                <th style={{ padding: '0.75rem 1rem', textAlign: 'center', fontSize: '1rem', fontWeight: 600 }}>Purchasers</th>
                                <th style={{ padding: '0.75rem 1rem', textAlign: 'center', fontSize: '1rem', fontWeight: 600 }}>View→Cart</th>
                                <th style={{ padding: '0.75rem 1rem', textAlign: 'center', fontSize: '1rem', fontWeight: 600 }}>Cart→Buy</th>
                                <th style={{ padding: '0.75rem 1rem', textAlign: 'center', fontSize: '1rem', fontWeight: 600 }}>Score</th>
                                <th style={{ padding: '0.75rem 1rem', textAlign: 'center', fontSize: '1rem', fontWeight: 600 }}>Health</th>
                            </tr>
                        </thead>
                        <tbody>
                            {data.map((row, idx) => (
                                <tr key={idx} style={{ borderBottom: '1px solid rgba(255, 255, 255, 0.05)' }}>
                                    <td style={{ padding: '0.75rem 1rem', fontSize: '1rem', color: '#9ca3af' }}>
                                        {new Date(row.window_start).toLocaleTimeString()}
                                    </td>
                                    <td style={{ padding: '0.75rem 1rem', textAlign: 'center', fontSize: '1rem' }}>{row.viewers}</td>
                                    <td style={{ padding: '0.75rem 1rem', textAlign: 'center', fontSize: '1rem' }}>{row.carters}</td>
                                    <td style={{ padding: '0.75rem 1rem', textAlign: 'center', fontSize: '1rem' }}>{row.purchasers}</td>
                                    <td style={{ padding: '0.75rem 1rem', textAlign: 'center', fontSize: '1rem' }}>
                                        <span style={{ whiteSpace: 'nowrap' }}>
                                            {row.view_to_cart_emoji}
                                        </span>
                                    </td>
                                    <td style={{ padding: '0.75rem 1rem', textAlign: 'center', fontSize: '1rem' }}>
                                        <span style={{ whiteSpace: 'nowrap' }}>
                                            {row.cart_to_buy_emoji}
                                        </span>
                                    </td>
                                    <td style={{ padding: '0.75rem 1rem', textAlign: 'center', fontSize: '1rem' }}>
                                        <span style={{ fontFamily: 'monospace' }}>{(row.funnel_score * 100).toFixed(1)}%</span>
                                    </td>
                                    <td style={{ padding: '0.75rem 1rem', textAlign: 'center' }}>
                                        <span
                                            style={{
                                                padding: '0.25rem 0.75rem',
                                                borderRadius: '0.25rem',
                                                fontSize: '0.875rem',
                                                fontWeight: 500,
                                                whiteSpace: 'nowrap',
                                                backgroundColor: `${getHealthColor(row.funnel_health)}20`,
                                                color: getHealthColor(row.funnel_health)
                                            }}
                                        >
                                            {row.funnel_health}
                                        </span>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                )}
            </div>

            {/* UDF Info */}
            <div className="glass-card" style={{ padding: '1.5rem' }}>
                <h3 className="text-lg font-semibold mb-3">Python UDFs in Use</h3>
                <div className="grid grid-cols-2 gap-4 text-sm">
                    <div className="p-3 rounded" style={{ background: 'rgba(99, 110, 250, 0.1)' }}>
                        <code className="text-purple-400">conversion_category(rate)</code>
                        <p className="text-gray-400 mt-1">Categorizes rates: excellent/good/average/needs_improvement</p>
                    </div>
                    <div className="p-3 rounded" style={{ background: 'rgba(99, 110, 250, 0.1)' }}>
                        <code className="text-purple-400">calculate_funnel_score(v,c,p)</code>
                        <p className="text-gray-400 mt-1">Weighted score: 40% view→cart, 60% cart→buy</p>
                    </div>
                    <div className="p-3 rounded" style={{ background: 'rgba(99, 110, 250, 0.1)' }}>
                        <code className="text-purple-400">format_rate_with_emoji(rate)</code>
                        <p className="text-gray-400 mt-1">Visual indicators: 🟢🟡🟠🔴</p>
                    </div>
                    <div className="p-3 rounded" style={{ background: 'rgba(99, 110, 250, 0.1)' }}>
                        <code className="text-purple-400">calculate_funnel_health(v2c, c2b)</code>
                        <p className="text-gray-400 mt-1">Health status: strong/moderate/weak</p>
                    </div>
                </div>
            </div>
        </motion.div>
    );
};

export default EnrichedFunnelTab;
