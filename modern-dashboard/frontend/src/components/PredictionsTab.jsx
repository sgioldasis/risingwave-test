import React, { useState, useEffect } from 'react';
import {
    AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
    BarChart, Bar, Cell, ReferenceLine, Legend
} from 'recharts';
import {
    Brain, TrendingUp, TrendingDown, Activity, Clock,
    Target, BarChart3, Zap, AlertCircle, CheckCircle2
} from 'lucide-react';
import { motion } from 'framer-motion';

const PredictionsTab = ({ funnelData }) => {
    const [predictions, setPredictions] = useState(null);
    const [predictionHistory, setPredictionHistory] = useState([]);
    const [modelStatus, setModelStatus] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [comparisonData, setComparisonData] = useState([]);
    const [timeAdjustedPredictions, setTimeAdjustedPredictions] = useState(null);
    const [minuteProgress, setMinuteProgress] = useState(null);
    const [isRefreshing, setIsRefreshing] = useState(false);

    // Helper to format timestamps - handles both ISO with timezone and without
    const formatTimestamp = (ts) => {
        if (!ts) return '-';
        // If timestamp already has timezone info (+00:00 or Z), don't append Z
        const cleanTs = ts.endsWith('Z') || ts.match(/[+-]\d{2}:\d{2}$/) ? ts : ts + 'Z';
        const date = new Date(cleanTs);
        return isNaN(date.getTime()) ? 'Invalid Date' : date.toLocaleTimeString();
    };

    // Helper to parse model version and extract training time
    const parseModelVersion = (version) => {
        if (!version || version === 'unknown') return null;
        // Version format: vYYYYMMDD_HHMMSS
        const match = version.match(/v(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})/);
        if (!match) return null;
        const [, year, month, day, hour, minute, second] = match;
        return new Date(`${year}-${month}-${day}T${hour}:${minute}:${second}Z`);
    };

    // Format model training time from version
    const formatModelTrainingTime = (version) => {
        const date = parseModelVersion(version);
        if (!date) return 'Unknown';
        return date.toLocaleTimeString();
    };

    // Get model age in minutes
    const getModelAgeMinutes = (version) => {
        const date = parseModelVersion(version);
        if (!date) return null;
        const now = new Date();
        const diffMs = now - date;
        return Math.floor(diffMs / 60000);
    };

    // Fetch predictions from API
    const fetchPredictions = async () => {
        setIsRefreshing(true);
        try {
            const [nextRes, historyRes, statusRes, compareRes] = await Promise.all([
                fetch('/api/predictions/next'),
                fetch('/api/predictions/history'),
                fetch('/api/predictions/status'),
                fetch('/api/predictions/comparison')
            ]);

            const nextData = await nextRes.json();
            const historyData = await historyRes.json();
            const statusData = await statusRes.json();
            const compareData = await compareRes.json();

            if (nextData.error) {
                setError(nextData.error);
            } else {
                setPredictions(nextData);
                setError(null);
            }

            setPredictionHistory(historyData.data || []);
            setModelStatus(statusData);
            
            // Store comparison data including time-adjusted predictions
            if (compareData.time_adjusted_predictions) {
                setTimeAdjustedPredictions(compareData.time_adjusted_predictions);
                setMinuteProgress(compareData.current_minute_progress);
            }
            
            setLoading(false);
        } catch (err) {
            setError(err.message);
            setLoading(false);
        } finally {
            setIsRefreshing(false);
        }
    };

    // Build comparison data when we have funnel data
    useEffect(() => {
        if (funnelData && funnelData.length > 0) {
            const combined = buildComparisonData(funnelData, predictions, timeAdjustedPredictions);
            setComparisonData(combined);
        }
    }, [predictions, funnelData, timeAdjustedPredictions]);

    useEffect(() => {
        // Initial fetch
        fetchPredictions();

        // Set up interval to refresh every 10 seconds
        const interval = setInterval(fetchPredictions, 10000);
        // Store interval ID on window to clear it later
        window._predictionInterval = interval;

        return () => {
            if (window._predictionInterval) {
                clearInterval(window._predictionInterval);
            }
        };
    }, []);

    const buildComparisonData = (actuals, preds, adjustedPreds) => {
        // Get last 10 actual data points with _actual keys for chart
        const recentActuals = actuals.slice(-10).map(d => ({
            ...d,
            viewers_actual: d.viewers,
            carters_actual: d.carters,
            purchasers_actual: d.purchasers,
            viewers_pred: null,
            carters_pred: null,
            purchasers_pred: null,
            type: 'actual'
        }));

        // Add prediction point if available
        // Chart shows FULL predictions on dotted lines (for next minute comparison)
        // adjustedPreds are only used for the percentage display in cards
        const predValues = preds && preds.viewers !== undefined ? preds : null;
            
        if (preds && !preds.error && preds.timestamp && predValues) {
            // Create a connecting point (last actual with prediction values)
            const lastActual = recentActuals[recentActuals.length - 1];
            const predPoint = {
                window_start: preds.timestamp,
                viewers_actual: null,  // Don't show for actual line
                viewers_pred: predValues.viewers,
                carters_actual: null,
                carters_pred: predValues.carters,
                purchasers_actual: null,
                purchasers_pred: predValues.purchasers,
                view_to_cart_rate: predValues.view_to_cart_rate,
                cart_to_buy_rate: predValues.cart_to_buy_rate,
                type: 'predicted'
            };
            
            // Add prediction values to last actual point for line connection
            if (lastActual) {
                lastActual.viewers_pred = predValues.viewers;
                lastActual.carters_pred = predValues.carters;
                lastActual.purchasers_pred = predValues.purchasers;
            }
            
            return [...recentActuals, predPoint];
        }

        return recentActuals;
    };


    const calculateChange = (predicted, actual) => {
        if (!actual || actual === 0) return 0;
        return ((predicted - actual) / actual) * 100;
    };

    if (loading) {
        return (
            <div className="flex items-center justify-center h-[400px]">
                <motion.div
                    animate={{ rotate: 360 }}
                    transition={{ repeat: Infinity, duration: 2, ease: "linear" }}
                >
                    <Brain size={48} className="text-purple-400" />
                </motion.div>
                <span className="ml-4 text-lg">Loading ML predictions...</span>
            </div>
        );
    }

    if (error) {
        return (
            <div className="glass-card p-8 text-center">
                <AlertCircle size={48} className="mx-auto mb-4 text-red-400" />
                <h3 className="text-xl font-semibold mb-2">Prediction Service Unavailable</h3>
                <p className="text-gray-400 mb-4">{error}</p>
                <p className="text-sm text-gray-500">
                    Make sure ML Serving service is running (port 8001) and models are trained via Dagster.
                </p>
            </div>
        );
    }

    const latest = predictions?.last_actual || {};
    const next = predictions || {};
    
    // Use time-adjusted predictions for comparison if available
    const adjusted = timeAdjustedPredictions || next;
    
    // Calculate predicted changes (using time-adjusted for fair comparison)
    const viewersChange = calculateChange(adjusted.viewers, latest.viewers);
    const cartersChange = calculateChange(adjusted.carters, latest.carters);
    const purchasersChange = calculateChange(adjusted.purchasers, latest.purchasers);

    return (
        <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            className="space-y-6"
        >
            {/* Header with Model Status */}
            <div className="flex justify-between items-center">
                <div className="flex items-center gap-3">
                    <Brain size={28} className="text-purple-400" />
                    <div>
                        <h2 className="text-xl font-semibold">ML Predictions</h2>
                        <p className="text-sm text-gray-400">
                            Next-minute forecasts using scikit-learn (RandomForest/LinearRegression)
                        </p>
                    </div>
                </div>
                <div className="flex items-center gap-4">
                    {modelStatus?.models && (
                        <div className="flex items-center gap-2 text-sm">
                            <CheckCircle2 size={16} className={modelStatus.total_models > 0 ? "text-green-400" : "text-gray-400"} />
                            <span>{modelStatus.total_models || 0} of {Object.keys(modelStatus.models).length} models trained</span>
                        </div>
                    )}
                    <button
                        onClick={async () => {
                            try {
                                const res = await fetch('/api/predictions/train', { method: 'POST' });
                                const data = await res.json();
                                if (data.success) {
                                    fetchPredictions();
                                }
                            } catch (e) {
                                console.error('Training failed:', e);
                            }
                        }}
                        className="px-3 py-1.5 bg-purple-600 hover:bg-purple-500 text-white text-sm rounded-lg transition-colors flex items-center gap-2"
                    >
                        <Brain size={14} />
                        Reload Models
                    </button>
                    <div className="flex flex-col items-end gap-1">
                        <div className="refresh-badge flex items-center gap-2">
                            {isRefreshing && (
                                <motion.div
                                    animate={{ rotate: 360 }}
                                    transition={{ repeat: Infinity, duration: 1, ease: "linear" }}
                                >
                                    <Clock size={14} className="text-purple-400" />
                                </motion.div>
                            )}
                            {!isRefreshing && <Clock size={14} />}
                            Predicted: {predictions?.predicted_at ? formatTimestamp(predictions.predicted_at) : '-'}
                        </div>
                        {predictions?.model_version && predictions.model_version !== 'unknown' && (
                            <div className="text-xs text-gray-500 flex items-center gap-1" title={`Model version: ${predictions.model_version}`}>
                                <span className="text-gray-400">Trained:</span>
                                <span className="text-purple-400 font-medium">
                                    {formatModelTrainingTime(predictions.model_version)}
                                </span>
                                {getModelAgeMinutes(predictions.model_version) !== null && (
                                    <span className="text-gray-500">
                                        ({getModelAgeMinutes(predictions.model_version)}m ago)
                                    </span>
                                )}
                            </div>
                        )}
                    </div>
                </div>
            </div>

            {/* User Activity Flow with Predictions - Moved to top */}
            <div className="glass-card">
                <div className="flex justify-between items-center mb-4">
                    <div>
                        <h3 className="text-lg font-semibold">User Activity Flow with Predictions</h3>
                        <p className="text-sm text-gray-400">
                            Solid = Actual history, Dotted = Full next-minute prediction
                        </p>
                    </div>
                </div>
                <div className="chart-container">
                    <ResponsiveContainer width="100%" height="100%">
                        <AreaChart data={comparisonData}>
                            <defs>
                                <linearGradient id="colorViewersActual" x1="0" y1="0" x2="0" y2="1">
                                    <stop offset="5%" stopColor="#636efa" stopOpacity={0.3} />
                                    <stop offset="95%" stopColor="#636efa" stopOpacity={0} />
                                </linearGradient>
                                <linearGradient id="colorCartersActual" x1="0" y1="0" x2="0" y2="1">
                                    <stop offset="5%" stopColor="#00cc96" stopOpacity={0.3} />
                                    <stop offset="95%" stopColor="#00cc96" stopOpacity={0} />
                                </linearGradient>
                                <linearGradient id="colorPurchasersActual" x1="0" y1="0" x2="0" y2="1">
                                    <stop offset="5%" stopColor="#ff6692" stopOpacity={0.3} />
                                    <stop offset="95%" stopColor="#ff6692" stopOpacity={0} />
                                </linearGradient>
                            </defs>
                            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" vertical={false} />
                            <XAxis
                                dataKey="window_start"
                                tick={{ fill: 'rgba(255,255,255,0.3)', fontSize: 10 }}
                                axisLine={false}
                                tickLine={false}
                                tickFormatter={(str) => {
                                    const date = new Date(str);
                                    return `${date.getHours()}:${date.getMinutes().toString().padStart(2, '0')}`;
                                }}
                            />
                            <YAxis
                                tick={{ fill: 'rgba(255,255,255,0.3)', fontSize: 10 }}
                                axisLine={false}
                                tickLine={false}
                            />
                            <Legend
                                verticalAlign="top"
                                height={36}
                                iconType="line"
                                formatter={(value) => {
                                    if (value.includes('actual')) return value.replace('_actual', ' (Actual)');
                                    if (value.includes('pred')) return value.replace('_pred', ' (Predicted)');
                                    return value;
                                }}
                            />
                            <Tooltip
                                contentStyle={{ background: '#1a1d29', border: '1px solid rgba(255,255,255,0.1)', borderRadius: '12px' }}
                                itemStyle={{ fontSize: '12px' }}
                                labelFormatter={(label) => {
                                    const date = new Date(label);
                                    return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
                                }}
                                formatter={(value, name) => {
                                    const displayName = name
                                        .replace('viewers_actual', 'Viewers (Actual)')
                                        .replace('viewers_pred', 'Viewers (Predicted)')
                                        .replace('carters_actual', 'Carters (Actual)')
                                        .replace('carters_pred', 'Carters (Predicted)')
                                        .replace('purchasers_actual', 'Purchasers (Actual)')
                                        .replace('purchasers_pred', 'Purchasers (Predicted)');
                                    return [Math.round(value), displayName];
                                }}
                            />
                            {/* Actual data - solid lines with gradient fill */}
                            <Area type="monotone" dataKey="viewers_actual" stroke="#636efa" strokeWidth={3} fillOpacity={1} fill="url(#colorViewersActual)" />
                            <Area type="monotone" dataKey="carters_actual" stroke="#00cc96" strokeWidth={3} fillOpacity={1} fill="url(#colorCartersActual)" />
                            <Area type="monotone" dataKey="purchasers_actual" stroke="#ff6692" strokeWidth={3} fillOpacity={1} fill="url(#colorPurchasersActual)" />
                            {/* Predicted data - dotted lines without fill */}
                            <Area type="monotone" dataKey="viewers_pred" stroke="#636efa" strokeWidth={2} strokeDasharray="5 5" fill="none" />
                            <Area type="monotone" dataKey="carters_pred" stroke="#00cc96" strokeWidth={2} strokeDasharray="5 5" fill="none" />
                            <Area type="monotone" dataKey="purchasers_pred" stroke="#ff6692" strokeWidth={2} strokeDasharray="5 5" fill="none" />
                        </AreaChart>
                    </ResponsiveContainer>
                </div>
            </div>

            {/* Minute Progress Bar */}
            {minuteProgress && (
                <div className="glass-card p-3">
                    <div className="flex justify-between items-center text-sm mb-2">
                        <span className="text-gray-400">Current Minute Progress</span>
                        <span className="text-purple-400 font-mono">
                            {minuteProgress.progress_percent}% ({minuteProgress.elapsed_seconds}s / 60s)
                        </span>
                    </div>
                    <div className="w-full h-2 bg-gray-700 rounded-full overflow-hidden">
                        <motion.div
                            className="h-full bg-gradient-to-r from-purple-500 to-blue-500"
                            initial={{ width: 0 }}
                            animate={{ width: `${minuteProgress.progress_percent}%` }}
                            transition={{ duration: 0.5 }}
                        />
                    </div>
                    <p className="text-xs text-gray-500 mt-2">
                        Predictions are prorated based on elapsed time for fair comparison
                    </p>
                </div>
            )}

            {/* Prediction Cards */}
            <div className="grid grid-cols-3 gap-4">
                <PredictionCard
                    label="Predicted Viewers"
                    value={adjusted.viewers}
                    fullValue={next.viewers}
                    change={viewersChange}
                    actual={latest.viewers}
                    color="#636efa"
                    icon={<Activity size={20} />}
                    isTimeAdjusted={!!timeAdjustedPredictions}
                />
                <PredictionCard
                    label="Predicted Carters"
                    value={adjusted.carters}
                    fullValue={next.carters}
                    change={cartersChange}
                    actual={latest.carters}
                    color="#00cc96"
                    icon={<Target size={20} />}
                    isTimeAdjusted={!!timeAdjustedPredictions}
                />
                <PredictionCard
                    label="Predicted Purchasers"
                    value={adjusted.purchasers}
                    fullValue={next.purchasers}
                    change={purchasersChange}
                    actual={latest.purchasers}
                    color="#ff6692"
                    icon={<Zap size={20} />}
                    isTimeAdjusted={!!timeAdjustedPredictions}
                />
            </div>

            {/* Secondary Charts Row */}
            <div className="grid grid-cols-2 gap-6">
                {/* Conversion Rate Predictions */}
                <div className="glass-card">
                    <h3 className="text-lg font-semibold mb-4">Predicted Conversion Rates</h3>
                    <div className="h-[250px]">
                        <ResponsiveContainer width="100%" height="100%">
                            <BarChart data={[
                                {
                                    name: 'View → Cart',
                                    actual: (latest.view_to_cart_rate || 0) * 100,
                                    predicted: (next.view_to_cart_rate || 0) * 100
                                },
                                {
                                    name: 'Cart → Buy',
                                    actual: (latest.cart_to_buy_rate || 0) * 100,
                                    predicted: (next.cart_to_buy_rate || 0) * 100
                                }
                            ]}>
                                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" />
                                <XAxis
                                    dataKey="name"
                                    tick={{ fill: 'rgba(255,255,255,0.5)', fontSize: 11 }}
                                    axisLine={false}
                                    tickLine={false}
                                />
                                <YAxis
                                    tick={{ fill: 'rgba(255,255,255,0.3)', fontSize: 10 }}
                                    tickFormatter={(v) => `${v.toFixed(0)}%`}
                                    axisLine={false}
                                    tickLine={false}
                                />
                                <Tooltip
                                    contentStyle={{
                                        background: '#1a1d29',
                                        border: '1px solid rgba(255,255,255,0.1)',
                                        borderRadius: '12px'
                                    }}
                                    formatter={(v) => [`${v.toFixed(2)}%`]}
                                />
                                <Bar dataKey="actual" name="Actual" fill="#636efa" radius={[4, 4, 0, 0]} />
                                <Bar dataKey="predicted" name="Predicted" fill="#a855f7" radius={[4, 4, 0, 0]} />
                            </BarChart>
                        </ResponsiveContainer>
                    </div>
                    <div className="mt-4 text-center text-sm text-gray-400">
                        Comparison of actual vs predicted conversion rates
                    </div>
                </div>

                {/* Model Status */}
                <div className="glass-card">
                    <h3 className="text-lg font-semibold mb-4">Model Status</h3>
                    <div className="space-y-3">
                        {modelStatus?.models && Object.entries(modelStatus.models).map(([name, info]) => (
                            <div
                                key={name}
                                className="flex items-center justify-between p-3 bg-white/5 rounded-lg"
                            >
                                <div className="flex items-center gap-2">
                                    <div className={`w-2 h-2 rounded-full ${
                                        info.trained ? 'bg-green-400' :
                                        info.scaler_trained ? 'bg-yellow-400 animate-pulse' :
                                        'bg-red-400'
                                    }`} />
                                    <span className="text-sm font-medium">
                                        {name}
                                    </span>
                                </div>
                                <span className={`text-xs px-2 py-1 rounded ${
                                    info.trained ? 'bg-green-400/20 text-green-400' :
                                    info.scaler_trained ? 'bg-yellow-400/20 text-yellow-400' :
                                    'bg-red-400/20 text-red-400'
                                }`}>
                                    {info.trained ? 'trained' : info.scaler_trained ? 'training' : 'untrained'}
                                </span>
                            </div>
                        ))}
                    </div>
                    <div className="mt-4 pt-4 border-t border-white/10">
                        <div className="flex items-center gap-2 text-sm text-gray-400">
                            <BarChart3 size={16} />
                            <span>Engine: scikit-learn</span>
                        </div>
                        <div className="flex items-center gap-2 text-sm text-gray-400 mt-1">
                            <Target size={16} />
                            <span>Model: RandomForest/LinearRegression</span>
                        </div>
                    </div>
                </div>
            </div>
        </motion.div>
    );
};

// Prediction Card Component
const PredictionCard = ({ label, value, fullValue, change, actual, color, icon, isTimeAdjusted }) => {
    const isPositive = change >= 0;
    const changeColor = isPositive ? 'text-green-400' : 'text-red-400';
    const TrendIcon = isPositive ? TrendingUp : TrendingDown;

    return (
        <motion.div
            whileHover={{ scale: 1.02 }}
            className="glass-card p-5 relative overflow-hidden"
        >
            <div
                className="absolute top-0 left-0 w-1 h-full"
                style={{ background: color }}
            />
            <div className="flex justify-between items-start mb-3">
                <span className="text-sm text-gray-400">{label}</span>
                <div className="p-2 rounded-lg" style={{ background: `${color}20` }}>
                    {React.cloneElement(icon, { style: { color } })}
                </div>
            </div>
            <div className="text-2xl font-bold mb-1">
                {value !== null && value !== undefined ? Math.round(value) : '—'}
            </div>
            {isTimeAdjusted && fullValue && (
                <div className="text-xs text-gray-500 mb-1">
                    Full minute: ~{Math.round(fullValue)}
                </div>
            )}
            <div className="flex items-center gap-2">
                <span className={`text-sm flex items-center gap-1 ${changeColor}`}>
                    <TrendIcon size={14} />
                    {change > 0 ? '+' : ''}{change.toFixed(1)}%
                </span>
                <span className="text-xs text-gray-500">
                    vs actual {actual || 0}
                </span>
            </div>
        </motion.div>
    );
};

export default PredictionsTab;
