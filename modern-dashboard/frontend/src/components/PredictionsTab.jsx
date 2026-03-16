import React, { useState, useEffect, useRef } from 'react';
import {
    AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
    Legend
} from 'recharts';
import {
    Brain, Users, ShoppingCart, CreditCard,
    AlertCircle, CheckCircle2, Calculator, Sparkles
} from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

// KPICard Component (same style as Dashboard)
const KPICard = ({ label, actual, predicted, icon, gradient }) => (
    <motion.div
        className="glass-card"
        style={{
            flex: 1,
            minWidth: 0,
            padding: '0.75rem 1rem'
        }}
        whileHover={{
            y: -4,
            boxShadow: '0 12px 30px rgba(0,0,0,0.5), 0 0 15px rgba(99, 110, 250, 0.2)'
        }}
        transition={{ type: 'spring', stiffness: 300 }}
    >
        <div className="flex justify-between items-start mb-1">
            <div className={`p-1.5 rounded-lg bg-white/5`}>
                {React.cloneElement(icon, { size: 16 })}
            </div>
        </div>
        <div className="kpi-label">{label}</div>
        <div className="kpi-value" style={{ fontSize: '1.4rem' }}>
            <AnimatePresence mode="wait">
                <motion.span
                    key={actual}
                    initial={{ opacity: 0, y: 10 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, y: -10 }}
                >
                    {typeof actual === 'number' ? Math.round(actual).toLocaleString() : actual}
                </motion.span>
            </AnimatePresence>
        </div>
        {predicted !== undefined && predicted !== null && (
            <div style={{ fontSize: '1rem', color: 'rgba(255,255,255,0.7)', marginTop: '0.25rem', fontWeight: 500 }}>
                Predicted: {typeof predicted === 'number' ? Math.round(predicted).toLocaleString() : predicted}
            </div>
        )}
    </motion.div>
);

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
    const [lastModelType, setLastModelType] = useState(null);
    const [modelVersionFlash, setModelVersionFlash] = useState(false);
    
    // Ref to store previous valid predictions for smooth transitions
    const prevPredictionsRef = useRef(null);
    // Ref to store stable chart data to prevent full redraws
    const chartDataRef = useRef([]);

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

    // Format model version for display (v20260305_125613 -> "5 Mar 2026, 14:57:03" in local time)
    const formatModelVersionDisplay = (version) => {
        if (version === 'online_learning') return 'Live';
        const date = parseModelVersion(version);
        if (!date) return version;
        const day = date.getDate();
        const month = date.toLocaleString('en-US', { month: 'short' });
        const year = date.getFullYear();
        const hours = String(date.getHours()).padStart(2, '0');
        const minutes = String(date.getMinutes()).padStart(2, '0');
        const seconds = String(date.getSeconds()).padStart(2, '0');
        return `${day} ${month} ${year}, ${hours}:${minutes}:${seconds}`;
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
                // Store previous valid predictions before updating
                if (predictions && !predictions.error) {
                    prevPredictionsRef.current = predictions;
                }
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
            
            // Flash the model type badge on every prediction update (every 20 seconds)
            // This indicates fresh prediction data has been received
            const newModelType = nextData.model_type;
            if (newModelType) {
                setModelVersionFlash(true);
                setTimeout(() => setModelVersionFlash(false), 1000);
                setLastModelType(newModelType);
            }
            
            setLoading(false);
        } catch (err) {
            setError(err.message);
            setLoading(false);
        } finally {
            setIsRefreshing(false);
        }
    };

    // Build comparison data when actuals or predictions change
    // Actuals come from funnelData (parent) - same as dashboard tab
    // Predictions come from predictions API - updated separately
    useEffect(() => {
        if (funnelData && funnelData.length > 0) {
            const combined = buildComparisonData(funnelData, predictions, timeAdjustedPredictions);
            // Only update state if data actually changed to prevent chart redraws
            const currentJson = JSON.stringify(chartDataRef.current);
            const newJson = JSON.stringify(combined);
            if (currentJson !== newJson) {
                chartDataRef.current = combined;
                setComparisonData(combined);
            }
        }
    }, [predictions, funnelData, timeAdjustedPredictions]);

    useEffect(() => {
        // Initial fetch
        fetchPredictions();

        // Set up interval to refresh every 20 seconds for predictions
        const interval = setInterval(fetchPredictions, 20000);
        // Store interval ID on window to clear it later
        window._predictionInterval = interval;

        return () => {
            if (window._predictionInterval) {
                clearInterval(window._predictionInterval);
            }
        };
    }, []);

    // Helper to extract minute-level key for comparison
    const getMinuteKey = (ts) => {
        if (!ts) return '';
        // Remove timezone and extract YYYY-MM-DDTHH:MM
        return ts.replace(/[+-]\d{2}:\d{2}$/, '').replace('Z', '').slice(0, 16);
    };

    const buildComparisonData = (actuals, preds, adjustedPreds) => {
        // Determine effective predictions with fallback for transition stability
        // Only use fallback for prediction values to prevent dashed lines from flickering
        const hasValidPreds = preds && !preds.error && preds.timestamp;
        const effectivePreds = hasValidPreds ? preds : prevPredictionsRef.current;
        
        // Note: Actuals come purely from funnelData (same update rate as dashboard tab)
        // Predictions are updated separately and merged here for display
        
        // Get last 10 actual data points with _actual keys for chart
        let recentActuals = actuals.slice(-10).map(d => ({
            ...d,
            viewers_actual: d.viewers,
            carters_actual: d.carters,
            purchasers_actual: d.purchasers,
            viewers_pred: null,
            carters_pred: null,
            purchasers_pred: null,
            type: 'actual'
        }));
        
        // Actuals come purely from funnelData (same as dashboard tab)
        // No merging with last_actual from predictions to keep updates consistent

        // Add prediction values to last actual point for line connection (immutably)
        // This creates the dotted line connection from actual to prediction
        const predValues = effectivePreds && effectivePreds.viewers !== undefined ? effectivePreds : null;
        if (effectivePreds && !effectivePreds.error && effectivePreds.timestamp && predValues && recentActuals.length > 0) {
            // Always add prediction values to the last actual point for line connection
            // The prediction point itself will be added separately after deduplication
            const lastActual = recentActuals[recentActuals.length - 1];
            const updatedLastActual = {
                ...lastActual,
                viewers_pred: predValues.viewers,
                carters_pred: predValues.carters,
                purchasers_pred: predValues.purchasers
            };
            recentActuals = [...recentActuals.slice(0, -1), updatedLastActual];
        }

        // Final deduplication: ensure no two data points share the same minute
        // This handles race conditions when minute changes
        // Iterate backwards to keep the last occurrence (prediction points come last)
        const seenMinutes = new Set();
        const deduplicated = [];
        
        for (let i = recentActuals.length - 1; i >= 0; i--) {
            const point = recentActuals[i];
            const minuteKey = getMinuteKey(point.window_start);
            if (!seenMinutes.has(minuteKey)) {
                seenMinutes.add(minuteKey);
                deduplicated.unshift(point); // Add to front to preserve order
            }
            // If we've seen this minute before, skip this point
            // (the last occurrence takes precedence - keeps prediction points)
        }
        
        // Add placeholder for next minute to shift x-axis and show new time label
        // This ensures the chart shows the upcoming minute even before actual data arrives
        // Use effectivePreds (with fallback to prevPredictionsRef) to prevent flickering
        if (effectivePreds && !effectivePreds.error) {
            const lastPoint = deduplicated[deduplicated.length - 1];
            const lastPointKey = lastPoint ? getMinuteKey(lastPoint.window_start) : null;
            
            // Determine the prediction timestamp to use
            let predTimestamp = effectivePreds.timestamp;
            let predValues = effectivePreds;
            
            // If effectivePreds timestamp is not in the future, calculate next minute
            // This happens during minute transitions when predictions haven't updated yet
            if (lastPoint && predTimestamp) {
                const predMinuteKey = getMinuteKey(predTimestamp);
                if (predMinuteKey <= lastPointKey) {
                    // Prediction is stale (same or older minute), calculate next minute
                    const lastDate = new Date(lastPoint.window_start);
                    lastDate.setMinutes(lastDate.getMinutes() + 1);
                    predTimestamp = lastDate.toISOString();
                }
            }
            
            // Only add if we have a valid timestamp that's different from last actual
            if (predTimestamp && lastPointKey) {
                const predMinuteKey = getMinuteKey(predTimestamp);
                if (predMinuteKey > lastPointKey) {
                    const placeholderPoint = {
                        window_start: predTimestamp,
                        viewers_actual: null,
                        viewers_pred: predValues.viewers,
                        carters_actual: null,
                        carters_pred: predValues.carters,
                        purchasers_actual: null,
                        purchasers_pred: predValues.purchasers,
                        view_to_cart_rate: predValues.view_to_cart_rate,
                        cart_to_buy_rate: predValues.cart_to_buy_rate,
                        type: 'predicted'
                    };
                    deduplicated.push(placeholderPoint);
                }
            }
        }
        
        return deduplicated;
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

    // Use funnelData for actual values (refreshes continually like Dashboard)
    // funnelData is [{ window_start: '...', viewers: X, carters: Y, purchasers: Z }, ...]
    const latestActual = funnelData?.[funnelData?.length - 1] || {};
    const latest = {
        viewers: latestActual?.viewers || 0,
        carters: latestActual?.carters || 0,
        purchasers: latestActual?.purchasers || 0
    };
    
    // Use effective predictions with fallback to previous (same logic as chart)
    const hasValidPreds = predictions && !predictions.error && predictions.timestamp;
    const effectivePreds = hasValidPreds ? predictions : prevPredictionsRef.current;

    return (
        <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            className="space-y-6"
        >
            {/* Header with Model Status */}
            <div className="flex justify-between items-center mb-4">
                <div className="flex flex-col">
                    <div className="flex items-center gap-2">
                        <Brain size={24} className="text-purple-400 relative -top-[1px]" />
                        <h2 className="text-xl font-semibold">ML Predictions</h2>
                    </div>
                </div>
                <div className="flex items-center gap-4">

                    <div className="flex flex-col items-end gap-1">
                        {/* Single Combined Model Type Badge */}
                        {predictions?.model_type && (
                            <div className={`refresh-badge ${modelVersionFlash ? 'flash' : ''} ${predictions.is_heuristic ? 'heuristic' : 'ml-model'}`}>
                                {predictions.is_heuristic ? (
                                    <>
                                        <Calculator size={14} className="text-amber-400" />
                                        <span className="text-amber-400">
                                            {predictions.mode === 'online' ? `Online Learning (${predictions.source}): ` : `Batch Model (${predictions.source}): `}
                                            {predictions.model_version === 'rate_proportional' ? 'Rate-Proportional' :
                                             predictions.model_version === 'ema' ? 'EMA' :
                                             `Moving Average${predictions.model_version && predictions.model_version !== 'heuristic' && predictions.model_version !== 'live' && predictions.model_version !== 'live_moving_average' && predictions.model_version !== 'moving_average' ? `: ${formatModelVersionDisplay(predictions.model_version)}` : ' (Live)'}`}
                                        </span>
                                    </>
                                ) : (
                                    <>
                                        <Sparkles size={14} className="text-purple-400" />
                                        <span>
                                            {predictions.model_type === 'RandomForestRegressor' ? `Batch Model (${predictions.source}): RandomForest - ${formatModelVersionDisplay(predictions.model_version)}` :
                                             predictions.model_type === 'LinearRegression' ? `Batch Model (${predictions.source}): Linear Regression - ${formatModelVersionDisplay(predictions.model_version)}` :
                                             predictions.model_type === 'river_kafka_online' ? `Online Learning (${predictions.source}): River Online (Kafka) - ${formatModelVersionDisplay(predictions.model_version)}` :
                                             predictions.model_type === 'moving_average_fallback' ? `Online Learning (${predictions.source}): River Online (Kafka) [MA Fallback] - ${formatModelVersionDisplay(predictions.model_version)}` :
                                             predictions.model_type === 'river_risingwave_online' ? `Online Learning (${predictions.source}): River Online (RisingWave) - Live [Moving Average]` :
                                             `${predictions.mode === 'online' ? 'Online Learning' : 'Batch Model'} (${predictions.source}): ${predictions.model_type} - ${formatModelVersionDisplay(predictions.model_version)}`}
                                        </span>
                                    </>
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
                    </div>
                </div>
                <div className="chart-container">
                    <ResponsiveContainer width="100%" height="100%">
                        <AreaChart data={comparisonData} margin={{ top: 5, right: 30, left: 5, bottom: 5 }}>
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
                                tickFormatter={(str, index) => {
                                    const date = new Date(str);
                                    return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', hour12: false });
                                }}
                                // Use interval 0 to show all ticks, but let recharts handle deduplication
                                interval={0}
                            />
                            <YAxis
                                tick={{ fill: 'rgba(255,255,255,0.3)', fontSize: 10 }}
                                axisLine={false}
                                tickLine={false}
                            />
                            <Legend
                                verticalAlign="top"
                                height={36}
                                iconType="plainline"
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
                                    return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', hour12: false });
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
                            <Area type="monotone" dataKey="viewers_actual" stroke="#636efa" strokeWidth={3} fillOpacity={1} fill="url(#colorViewersActual)" isAnimationActive={false} />
                            <Area type="monotone" dataKey="carters_actual" stroke="#00cc96" strokeWidth={3} fillOpacity={1} fill="url(#colorCartersActual)" isAnimationActive={false} />
                            <Area type="monotone" dataKey="purchasers_actual" stroke="#ff6692" strokeWidth={3} fillOpacity={1} fill="url(#colorPurchasersActual)" isAnimationActive={false} />
                            {/* Predicted data - dotted lines without fill */}
                            <Area type="monotone" dataKey="viewers_pred" stroke="#636efa" strokeWidth={2} strokeDasharray="5 5" fill="none" isAnimationActive={false} />
                            <Area type="monotone" dataKey="carters_pred" stroke="#00cc96" strokeWidth={2} strokeDasharray="5 5" fill="none" isAnimationActive={false} />
                            <Area type="monotone" dataKey="purchasers_pred" stroke="#ff6692" strokeWidth={2} strokeDasharray="5 5" fill="none" isAnimationActive={false} />
                        </AreaChart>
                    </ResponsiveContainer>
                </div>
            </div>

            {/* Prediction Cards - KPICard Style */}
            <div style={{ display: 'flex', gap: '1rem', marginTop: '1.5rem' }}>
                <KPICard
                    label="Viewers"
                    actual={latest.viewers}
                    predicted={effectivePreds?.viewers}
                    icon={<Users />}
                    gradient="gradient-viewers"
                />
                <KPICard
                    label="Carters"
                    actual={latest.carters}
                    predicted={effectivePreds?.carters}
                    icon={<ShoppingCart />}
                    gradient="gradient-carters"
                />
                <KPICard
                    label="Purchasers"
                    actual={latest.purchasers}
                    predicted={effectivePreds?.purchasers}
                    icon={<CreditCard />}
                    gradient="gradient-purchasers"
                />
            </div>
        </motion.div>
    );
};

export default PredictionsTab;
