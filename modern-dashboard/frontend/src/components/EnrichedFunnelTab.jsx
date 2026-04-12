import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { TrendingUp, TrendingDown, Activity, BarChart3 } from 'lucide-react';

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
            <div style={{ padding: '1.5rem' }}>
                <div style={{
                    background: 'rgba(255, 255, 255, 0.03)',
                    backdropFilter: 'blur(10px)',
                    borderRadius: '1rem',
                    padding: '2rem',
                    border: '1px solid rgba(255, 255, 255, 0.08)',
                    textAlign: 'center'
                }}>
                    <Activity size={32} className="animate-pulse" style={{ color: '#636efa' }} />
                    <p style={{ marginTop: '1rem', color: 'rgba(255, 255, 255, 0.5)' }}>Loading enriched funnel data...</p>
                </div>
            </div>
        );
    }

    if (error) {
        return (
            <div style={{ padding: '1.5rem' }}>
                <div style={{
                    background: 'rgba(255, 255, 255, 0.03)',
                    backdropFilter: 'blur(10px)',
                    borderRadius: '1rem',
                    padding: '2rem',
                    border: '1px solid rgba(255, 255, 255, 0.08)',
                    textAlign: 'center'
                }}>
                    <div style={{ color: '#ef4444', marginBottom: '1rem' }}>
                        ⚠️ Error loading enriched data
                    </div>
                    <p style={{ color: 'rgba(255, 255, 255, 0.5)', fontSize: '0.875rem' }}>{error}</p>
                    <p style={{ color: 'rgba(255, 255, 255, 0.4)', fontSize: '0.75rem', marginTop: '1rem' }}>
                        Make sure Python UDFs are enabled and funnel_enriched MV exists
                    </p>
                </div>
            </div>
        );
    }

    return (
        <div style={{ padding: '1.5rem' }}>
            {/* Health Summary */}
            {Object.keys(health).length > 0 && (
                <motion.div
                    initial={{ opacity: 0, y: 10 }}
                    animate={{ opacity: 1, y: 0 }}
                    style={{
                        background: 'rgba(255, 255, 255, 0.03)',
                        backdropFilter: 'blur(10px)',
                        borderRadius: '1rem',
                        padding: '1.5rem',
                        border: '1px solid rgba(255, 255, 255, 0.08)',
                        marginBottom: '1.5rem'
                    }}
                >
                    <div style={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: '0.5rem',
                        marginBottom: '1rem',
                        color: 'rgba(255, 255, 255, 0.9)',
                        fontSize: '1.1rem',
                        fontWeight: 600
                    }}>
                        <BarChart3 size={20} />
                        <span>Funnel Health (Last 5 Minutes)</span>
                    </div>
                    <div style={{
                        display: 'grid',
                        gridTemplateColumns: 'repeat(3, 1fr)',
                        gap: '1rem'
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
                                        background: 'rgba(255, 255, 255, 0.03)',
                                        borderRadius: '0.75rem',
                                        padding: '1rem',
                                        border: '1px solid rgba(255, 255, 255, 0.08)',
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
                                            height: '2px',
                                            background: config.color
                                        }}
                                    />
                                    
                                    <Icon size={20} style={{ color: config.color, marginBottom: '0.5rem' }} />
                                    
                                    <div style={{
                                        fontSize: '0.65rem',
                                        color: 'rgba(255, 255, 255, 0.4)',
                                        textTransform: 'uppercase',
                                        letterSpacing: '0.05em',
                                        marginBottom: '0.25rem'
                                    }}>
                                        {config.label}
                                    </div>
                                    
                                    <div style={{
                                        fontSize: '1.5rem',
                                        fontWeight: 600,
                                        color: 'rgba(255, 255, 255, 0.9)'
                                    }}>
                                        {value.count}
                                    </div>
                                    
                                    <div style={{
                                        fontSize: '0.75rem',
                                        marginTop: '0.25rem',
                                        color: 'rgba(255, 255, 255, 0.4)'
                                    }}>
                                        Avg: {(value.avg_score * 100).toFixed(1)}%
                                    </div>
                                </motion.div>
                            );
                        })}
                    </div>
                </motion.div>
            )}

            {/* Data Table */}
            <motion.div
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.1 }}
                style={{
                    background: 'rgba(255, 255, 255, 0.03)',
                    backdropFilter: 'blur(10px)',
                    borderRadius: '1rem',
                    border: '1px solid rgba(255, 255, 255, 0.08)',
                    overflow: 'hidden'
                }}
            >
                <div style={{
                    padding: '1rem 1.5rem',
                    borderBottom: '1px solid rgba(255, 255, 255, 0.08)',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between'
                }}>
                    <div style={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: '0.5rem',
                        color: 'rgba(255, 255, 255, 0.9)',
                        fontWeight: 600
                    }}>
                        <Activity size={18} />
                        <span>Recent Funnel Windows</span>
                    </div>
                    <span style={{
                        fontSize: '0.875rem',
                        color: 'rgba(255, 255, 255, 0.5)'
                    }}>
                        {data.length} records
                    </span>
                </div>
                <div style={{ overflowX: 'auto' }}>
                    {data.length === 0 ? (
                        <div style={{
                            padding: '4rem',
                            textAlign: 'center',
                            color: 'rgba(255, 255, 255, 0.5)'
                        }}>
                            <Activity size={48} style={{ marginBottom: '1rem', opacity: 0.3 }} />
                            <p>No enriched data available</p>
                        </div>
                    ) : (
                        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                            <thead>
                                <tr style={{
                                    background: 'rgba(255, 255, 255, 0.02)',
                                    borderBottom: '1px solid rgba(255, 255, 255, 0.08)'
                                }}>
                                    <th style={{
                                        padding: '0.875rem 1.5rem',
                                        textAlign: 'left',
                                        fontSize: '0.75rem',
                                        color: 'rgba(255, 255, 255, 0.4)',
                                        textTransform: 'uppercase',
                                        letterSpacing: '0.05em',
                                        fontWeight: 500
                                    }}>Time</th>
                                    <th style={{
                                        padding: '0.875rem 1.5rem',
                                        textAlign: 'right',
                                        fontSize: '0.75rem',
                                        color: '#636efa',
                                        textTransform: 'uppercase',
                                        letterSpacing: '0.05em',
                                        fontWeight: 500
                                    }}>Viewers</th>
                                    <th style={{
                                        padding: '0.875rem 1.5rem',
                                        textAlign: 'right',
                                        fontSize: '0.75rem',
                                        color: '#00cc96',
                                        textTransform: 'uppercase',
                                        letterSpacing: '0.05em',
                                        fontWeight: 500
                                    }}>Carters</th>
                                    <th style={{
                                        padding: '0.875rem 1.5rem',
                                        textAlign: 'right',
                                        fontSize: '0.75rem',
                                        color: '#ff6692',
                                        textTransform: 'uppercase',
                                        letterSpacing: '0.05em',
                                        fontWeight: 500
                                    }}>Purchasers</th>
                                    <th style={{
                                        padding: '0.875rem 1.5rem',
                                        textAlign: 'center',
                                        fontSize: '0.75rem',
                                        color: 'rgba(255, 255, 255, 0.4)',
                                        textTransform: 'uppercase',
                                        letterSpacing: '0.05em',
                                        fontWeight: 500
                                    }}>View→Cart</th>
                                    <th style={{
                                        padding: '0.875rem 1.5rem',
                                        textAlign: 'center',
                                        fontSize: '0.75rem',
                                        color: 'rgba(255, 255, 255, 0.4)',
                                        textTransform: 'uppercase',
                                        letterSpacing: '0.05em',
                                        fontWeight: 500
                                    }}>Cart→Buy</th>
                                    <th style={{
                                        padding: '0.875rem 1.5rem',
                                        textAlign: 'center',
                                        fontSize: '0.75rem',
                                        color: 'rgba(255, 255, 255, 0.4)',
                                        textTransform: 'uppercase',
                                        letterSpacing: '0.05em',
                                        fontWeight: 500
                                    }}>Score</th>
                                    <th style={{
                                        padding: '0.875rem 1.5rem',
                                        textAlign: 'center',
                                        fontSize: '0.75rem',
                                        color: 'rgba(255, 255, 255, 0.4)',
                                        textTransform: 'uppercase',
                                        letterSpacing: '0.05em',
                                        fontWeight: 500
                                    }}>Health</th>
                                </tr>
                            </thead>
                            <tbody>
                                {data.map((row, idx) => (
                                    <motion.tr
                                        key={idx}
                                        initial={{ opacity: 0 }}
                                        animate={{ opacity: 1 }}
                                        transition={{ delay: idx * 0.02 }}
                                        style={{
                                            borderBottom: '1px solid rgba(255, 255, 255, 0.04)'
                                        }}
                                    >
                                        <td style={{
                                            padding: '0.875rem 1.5rem',
                                            fontSize: '0.875rem',
                                            color: 'rgba(255, 255, 255, 0.8)'
                                        }}>
                                            {new Date(row.window_start).toLocaleTimeString()}
                                        </td>
                                        <td style={{
                                            padding: '0.875rem 1.5rem',
                                            textAlign: 'right',
                                            fontSize: '0.875rem',
                                            color: '#636efa',
                                            fontWeight: 500
                                        }}>{row.viewers?.toLocaleString() || 0}</td>
                                        <td style={{
                                            padding: '0.875rem 1.5rem',
                                            textAlign: 'right',
                                            fontSize: '0.875rem',
                                            color: '#00cc96',
                                            fontWeight: 500
                                        }}>{row.carters?.toLocaleString() || 0}</td>
                                        <td style={{
                                            padding: '0.875rem 1.5rem',
                                            textAlign: 'right',
                                            fontSize: '0.875rem',
                                            color: '#ff6692',
                                            fontWeight: 500
                                        }}>{row.purchasers?.toLocaleString() || 0}</td>
                                        <td style={{
                                            padding: '0.875rem 1.5rem',
                                            textAlign: 'center',
                                            fontSize: '0.875rem',
                                            color: 'rgba(255, 255, 255, 0.8)'
                                        }}>
                                            <span style={{ whiteSpace: 'nowrap' }}>
                                                {row.view_to_cart_emoji}
                                            </span>
                                        </td>
                                        <td style={{
                                            padding: '0.875rem 1.5rem',
                                            textAlign: 'center',
                                            fontSize: '0.875rem',
                                            color: 'rgba(255, 255, 255, 0.8)'
                                        }}>
                                            <span style={{ whiteSpace: 'nowrap' }}>
                                                {row.cart_to_buy_emoji}
                                            </span>
                                        </td>
                                        <td style={{
                                            padding: '0.875rem 1.5rem',
                                            textAlign: 'center',
                                            fontSize: '0.875rem',
                                            color: 'rgba(255, 255, 255, 0.8)',
                                            fontFamily: 'monospace'
                                        }}>
                                            {(row.funnel_score * 100).toFixed(1)}%
                                        </td>
                                        <td style={{ padding: '0.875rem 1.5rem', textAlign: 'center' }}>
                                            <span
                                                style={{
                                                    padding: '0.25rem 0.5rem',
                                                    borderRadius: '0.25rem',
                                                    fontSize: '0.75rem',
                                                    fontWeight: 500,
                                                    whiteSpace: 'nowrap',
                                                    backgroundColor: `${getHealthColor(row.funnel_health)}20`,
                                                    color: getHealthColor(row.funnel_health)
                                                }}
                                            >
                                                {row.funnel_health}
                                            </span>
                                        </td>
                                    </motion.tr>
                                ))}
                            </tbody>
                        </table>
                    )}
                </div>
            </motion.div>

            {/* UDF Info */}
            <motion.div
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.2 }}
                style={{
                    background: 'rgba(255, 255, 255, 0.03)',
                    backdropFilter: 'blur(10px)',
                    borderRadius: '1rem',
                    padding: '1.5rem',
                    border: '1px solid rgba(255, 255, 255, 0.08)'
                }}
            >
                <div style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.5rem',
                    marginBottom: '1rem',
                    color: 'rgba(255, 255, 255, 0.9)',
                    fontWeight: 600
                }}>
                    <Activity size={18} />
                    <span>Python UDFs in Use</span>
                </div>
                <div style={{
                    display: 'grid',
                    gridTemplateColumns: 'repeat(2, 1fr)',
                    gap: '1rem'
                }}>
                    <div style={{
                        padding: '0.75rem',
                        borderRadius: '0.5rem',
                        background: 'rgba(99, 110, 250, 0.1)'
                    }}>
                        <code style={{ color: '#a78bfa', fontSize: '0.875rem' }}>conversion_category(rate)</code>
                        <p style={{ color: 'rgba(255, 255, 255, 0.4)', marginTop: '0.25rem', fontSize: '0.875rem' }}>
                            Categorizes rates: excellent/good/average/needs_improvement
                        </p>
                    </div>
                    <div style={{
                        padding: '0.75rem',
                        borderRadius: '0.5rem',
                        background: 'rgba(99, 110, 250, 0.1)'
                    }}>
                        <code style={{ color: '#a78bfa', fontSize: '0.875rem' }}>calculate_funnel_score(v,c,p)</code>
                        <p style={{ color: 'rgba(255, 255, 255, 0.4)', marginTop: '0.25rem', fontSize: '0.875rem' }}>
                            Weighted score: 40% view→cart, 60% cart→buy
                        </p>
                    </div>
                    <div style={{
                        padding: '0.75rem',
                        borderRadius: '0.5rem',
                        background: 'rgba(99, 110, 250, 0.1)'
                    }}>
                        <code style={{ color: '#a78bfa', fontSize: '0.875rem' }}>format_rate_with_emoji(rate)</code>
                        <p style={{ color: 'rgba(255, 255, 255, 0.4)', marginTop: '0.25rem', fontSize: '0.875rem' }}>
                            Visual indicators: 🟢🟡🟠🔴
                        </p>
                    </div>
                    <div style={{
                        padding: '0.75rem',
                        borderRadius: '0.5rem',
                        background: 'rgba(99, 110, 250, 0.1)'
                    }}>
                        <code style={{ color: '#a78bfa', fontSize: '0.875rem' }}>calculate_funnel_health(v2c, c2b)</code>
                        <p style={{ color: 'rgba(255, 255, 255, 0.4)', marginTop: '0.25rem', fontSize: '0.875rem' }}>
                            Health status: strong/moderate/weak
                        </p>
                    </div>
                </div>
            </motion.div>
        </div>
    );
};

export default EnrichedFunnelTab;
