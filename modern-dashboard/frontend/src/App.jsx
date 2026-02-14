// Updated: 2026-02-01 07:15:51
import React, { useState, useEffect, useRef } from 'react';
import {
    LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, AreaChart, Area,
    BarChart, Bar, Cell
} from 'recharts';
import {
    TrendingUp, TrendingDown, Users, ShoppingCart, CreditCard,
    Activity, Zap, Clock, Maximize2, MoreHorizontal
} from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import PureCSSFunnel from './components/PureCSSFunnel';

const isWebGLAvailable = () => {
    try {
        const canvas = document.createElement('canvas');
        return !!(window.WebGLRenderingContext && (canvas.getContext('webgl') || canvas.getContext('experimental-webgl')));
    } catch (e) {
        return false;
    }
};

// Force disable WebGL due to Three.js compatibility issues
const hasWebGL = false;
const ThreeDFunnel = null;


const App = () => {
    const [data, setData] = useState([]);
    const [stats, setStats] = useState(null);
    const [loading, setLoading] = useState(true);
    const [lastUpdate, setLastUpdate] = useState(new Date());
    const [lastEventTime, setLastEventTime] = useState(null);
    const [activeTab, setActiveTab] = useState('dashboard');
    const logContainerRef = useRef(null);
    const logEndRef = useRef(null);

    // Clickstream data state
    const [clickstreamData, setClickstreamData] = useState([]);
    const [clickstreamSummary, setClickstreamSummary] = useState({
        page_views: 0, clicks: 0, add_to_carts: 0, checkouts: 0, purchases: 0, revenue: 0
    });
    const [recentSessions, setRecentSessions] = useState([]);
    
    // Track updates for flash animation
    const [flashCards, setFlashCards] = useState({});
    const [flashSessions, setFlashSessions] = useState({});
    const prevSummaryRef = useRef({});
    const prevSessionsRef = useRef([]);

    const [producerStatus, setProducerStatus] = useState({ running: false, output: [] });

    // Auto-scroll to show last line with padding below
    useEffect(() => {
        if (logEndRef.current) {
            // Scroll to the ref element, which is after the last log line
            logEndRef.current.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
        }
    }, [producerStatus.output]);

    const fetchData = async () => {
        try {
            const [funnelRes, statsRes, producerRes, lastEventRes, clickstreamRes, clickstreamSummaryRes, sessionsRes] = await Promise.all([
                fetch('/api/funnel'),
                fetch('/api/stats'),
                fetch('/api/producer/status'),
                fetch('/api/last-event-time'),
                fetch('/api/clickstream/events-by-time'),
                fetch('/api/clickstream/event-summary'),
                fetch('/api/clickstream/recent-sessions')
            ]);
            const funnelData = await funnelRes.json();
            const statsData = await statsRes.json();
            const prodData = await producerRes.json();
            const lastEventData = await lastEventRes.json();
            const clickstreamData = await clickstreamRes.json();
            const clickstreamSummaryData = await clickstreamSummaryRes.json();
            const sessionsData = await sessionsRes.json();

            // Process for chart (recharts needs oldest first)
            setData([...funnelData].reverse());
            setStats(statsData);
            setProducerStatus(prodData);
            setLastUpdate(new Date());
            if (lastEventData.last_event_time) {
                setLastEventTime(new Date(lastEventData.last_event_time));
            }
            // Set clickstream data
            if (!clickstreamData.error) {
                setClickstreamData([...clickstreamData].reverse());
            }
            if (!clickstreamSummaryData.error) {
                // Check which cards have updates
                const newFlashCards = {};
                const prevSummary = prevSummaryRef.current;
                ['page_views', 'clicks', 'add_to_carts', 'checkouts', 'purchases', 'revenue'].forEach(key => {
                    if (prevSummary[key] !== undefined && clickstreamSummaryData[key] !== prevSummary[key]) {
                        newFlashCards[key] = true;
                    }
                });
                if (Object.keys(newFlashCards).length > 0) {
                    setFlashCards(newFlashCards);
                    setTimeout(() => setFlashCards({}), 500);
                }
                prevSummaryRef.current = clickstreamSummaryData;
                setClickstreamSummary(clickstreamSummaryData);
            }
            if (!sessionsData.error) {
                // Check which sessions are new or have revenue updates
                const newFlashSessions = {};
                const prevSessions = prevSessionsRef.current;
                const prevSessionIds = new Set(prevSessions.map(s => s.session_id));
                
                sessionsData.forEach((session) => {
                    const prevSession = prevSessions.find(s => s.session_id === session.session_id);
                    // Flash if it's a new session OR if revenue changed
                    if (!prevSession) {
                        newFlashSessions[session.session_id] = 'new';
                    } else if (prevSession.total_revenue !== session.total_revenue) {
                        newFlashSessions[session.session_id] = 'updated';
                    }
                });
                
                // Store flash sessions for sorting before clearing
                const flashSessionsForSort = { ...newFlashSessions };
                
                // Update flash sessions state
                if (Object.keys(newFlashSessions).length > 0) {
                    setFlashSessions(newFlashSessions);
                    setTimeout(() => setFlashSessions({}), 800);
                }
                prevSessionsRef.current = sessionsData;
                
                // Sort sessions: updated/new first, then by session_start desc
                const sortedSessions = [...sessionsData].sort((a, b) => {
                    const aFlash = flashSessionsForSort[a.session_id];
                    const bFlash = flashSessionsForSort[b.session_id];
                    if (aFlash && !bFlash) return -1;
                    if (!aFlash && bFlash) return 1;
                    return new Date(b.session_start) - new Date(a.session_start);
                });
                
                setRecentSessions(sortedSessions);
            }
            setLoading(false);
        } catch (error) {
            console.error('Error fetching data:', error);
        }
    };

    const startProducer = async () => {
        await fetch('/api/producer/start', { method: 'POST' });
        fetchData();
    };

    const stopProducer = async () => {
        await fetch('/api/producer/stop', { method: 'POST' });
        fetchData();
    };

    useEffect(() => {
        fetchData();
        const interval = setInterval(fetchData, 2000);
        return () => clearInterval(interval);
    }, []);

    if (loading) {
        return (
            <div className="flex items-center justify-center h-screen w-full text-2xl font-bold gradient-text">
                <motion.div
                    animate={{ scale: [1, 1.1, 1] }}
                    transition={{ repeat: Infinity, duration: 1.5 }}
                >
                    Initializing Nebula Dashboard...
                </motion.div>
            </div>
        );
    }

    const latest = stats?.latest || {};
    const changes = stats?.changes || {};

    const funnelData = [
        { name: 'Viewers', value: latest.viewers, color: '#636efa', depth: 100 },
        { name: 'Carters', value: latest.carters, color: '#00cc96', depth: 80 },
        { name: 'Purchasers', value: latest.purchasers, color: '#ff6692', depth: 60 },
    ];

    return (
        <div className={`dashboard-grid tab-${activeTab}`}>
            <header>
                <div className="logo">
                    <img src="/kaizengaming-logo.png" alt="Kaizen Gaming Logo" className="h-8 w-8 rounded-lg shadow-lg" style={{ height: '32px', width: '32px', objectFit: 'cover' }} />
                    <span className="brand gradient-text" style={{ fontSize: '1.5rem', fontWeight: 800 }}>Kaizen Gaming</span>
                    <span style={{ color: 'rgba(255,255,255,0.3)', fontWeight: 400 }}>|</span>
                    <span style={{ fontSize: '1.1rem', opacity: 0.8, fontWeight: 600, letterSpacing: '-0.01em' }}>Real-time Dashboard</span>
                </div>

                <div className="header-center">
                <nav className="tabs-container">
                    <button
                        onClick={() => setActiveTab('dashboard')}
                        className={`tab-item ${activeTab === 'dashboard' ? 'active' : ''}`}
                    >
                        {activeTab === 'dashboard' && (
                            <motion.div
                                layoutId="activeTab"
                                className="tab-active-pill"
                                style={{ position: 'absolute', inset: '4px', zIndex: -1 }}
                                transition={{ type: 'spring', bounce: 0.2, duration: 0.6 }}
                            />
                        )}
                        Dashboard
                    </button>
                    <button
                        onClick={() => setActiveTab('clickstream')}
                        className={`tab-item ${activeTab === 'clickstream' ? 'active' : ''}`}
                    >
                        {activeTab === 'clickstream' && (
                            <motion.div
                                layoutId="activeTab"
                                className="tab-active-pill"
                                style={{ position: 'absolute', inset: '4px', zIndex: -1 }}
                                transition={{ type: 'spring', bounce: 0.2, duration: 0.6 }}
                            />
                        )}
                        Clickstream
                    </button>
                    <button
                        onClick={() => setActiveTab('producer')}
                        className={`tab-item ${activeTab === 'producer' ? 'active' : ''}`}
                    >
                        {activeTab === 'producer' && (
                            <motion.div
                                layoutId="activeTab"
                                className="tab-active-pill"
                                style={{ position: 'absolute', inset: '4px', zIndex: -1 }}
                                transition={{ type: 'spring', bounce: 0.2, duration: 0.6 }}
                            />
                        )}
                        Producer
                        <span className={`ml-2 w-2 h-2 rounded-full ${producerStatus.running ? 'bg-[#00cc96] animate-pulse' : 'bg-white/20'}`} />
                    </button>
                    <div className="flex gap-2 ml-2 items-center">
                        <button
                            onClick={startProducer}
                            className="btn-primary btn-compact"
                            disabled={producerStatus.running}
                        >
                            Start
                        </button>
                        <button
                            onClick={stopProducer}
                            className="btn-secondary btn-compact"
                            disabled={!producerStatus.running}
                        >
                            Stop
                        </button>
                    </div>
                </nav>
                </div>

                <div className="header-right">
                    <div
                        className="refresh-badge"
                        style={{
                            background: producerStatus.running ? 'rgba(0, 204, 150, 0.1)' : 'rgba(239, 68, 68, 0.1)',
                            borderColor: producerStatus.running ? 'rgba(0, 204, 150, 0.2)' : 'rgba(239, 68, 68, 0.2)',
                            color: producerStatus.running ? '#00cc96' : '#ef4444',
                            marginRight: '0.75rem'
                        }}
                    >
                        <span
                            className={`w-2 h-2 rounded-full ${producerStatus.running ? 'animate-pulse' : ''}`}
                            style={{ background: producerStatus.running ? '#00cc96' : '#ef4444' }}
                        />
                        Status: {producerStatus.running ? 'Running' : 'Stopped'}
                    </div>
                    <div className="refresh-badge">
                        <Clock size={14} />
                        {lastUpdate.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' })}, {lastUpdate.toLocaleTimeString()}
                    </div>
                </div>
            </header>

            <AnimatePresence mode="wait">
                {activeTab === 'dashboard' ? (
                    <motion.div
                        key="dashboard"
                        initial={{ opacity: 0, scale: 0.98 }}
                        animate={{ opacity: 1, scale: 1 }}
                        exit={{ opacity: 0, scale: 1.02 }}
                        className="dashboard-content-grid"
                    >
                        {/* KPI Cards */}
                        <KPICard
                            label="Total Viewers"
                            value={latest.viewers}
                            change={changes.viewers}
                            icon={<Users size={20} />}
                            gradient="gradient-viewers"
                        />
                        <KPICard
                            label="Active Carters"
                            value={latest.carters}
                            change={changes.carters}
                            icon={<ShoppingCart size={20} />}
                            gradient="gradient-carters"
                        />
                        <KPICard
                            label="Purchasers"
                            value={latest.purchasers}
                            change={changes.purchasers}
                            icon={<CreditCard size={20} />}
                            gradient="gradient-purchasers"
                        />
                        <KPICard
                            label="Cart Rate"
                            value={`${(latest.view_to_cart_rate * 100).toFixed(1)}%`}
                            change={changes.view_to_cart_rate}
                            icon={<Zap size={20} />}
                            gradient="blue"
                        />

                        {/* Main Charts Row */}
                        <div className="col-8 glass-card" style={{ perspective: '1000px' }}>
                            <div className="flex justify-between items-center mb-3">
                                <h3 className="m-0 text-base font-semibold">User Activity Flow</h3>
                                <div className="flex gap-2">
                                    <span className="refresh-badge"><Activity size={12} /> {lastEventTime ? `Last event: ${lastEventTime.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' })}, ${lastEventTime.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false })}` : 'No events'}</span>
                                </div>
                            </div>
                            <motion.div
                                className="chart-container"
                                initial={{ rotateX: 10 }}
                                whileHover={{ rotateX: 0 }}
                                transition={{ type: 'spring', stiffness: 300 }}
                            >
                                <ResponsiveContainer width="100%" height="100%">
                                    <AreaChart data={data}>
                                        <defs>
                                            <linearGradient id="colorViewers" x1="0" y1="0" x2="0" y2="1">
                                                <stop offset="5%" stopColor="#636efa" stopOpacity={0.3} />
                                                <stop offset="95%" stopColor="#636efa" stopOpacity={0} />
                                            </linearGradient>
                                            <linearGradient id="colorCarters" x1="0" y1="0" x2="0" y2="1">
                                                <stop offset="5%" stopColor="#00cc96" stopOpacity={0.3} />
                                                <stop offset="95%" stopColor="#00cc96" stopOpacity={0} />
                                            </linearGradient>
                                            <linearGradient id="colorPurchasers" x1="0" y1="0" x2="0" y2="1">
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
                                        <Tooltip
                                            contentStyle={{ background: '#1a1d29', border: '1px solid rgba(255,255,255,0.1)', borderRadius: '12px' }}
                                            itemStyle={{ fontSize: '12px' }}
                                            labelFormatter={(label) => {
                                                const date = new Date(label);
                                                return `${date.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' })}, ${date.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false })}`;
                                            }}
                                        />
                                        <Area type="monotone" dataKey="viewers" stroke="#636efa" strokeWidth={3} fillOpacity={1} fill="url(#colorViewers)" />
                                        <Area type="monotone" dataKey="carters" stroke="#00cc96" strokeWidth={3} fillOpacity={1} fill="url(#colorCarters)" />
                                        <Area type="monotone" dataKey="purchasers" stroke="#ff6692" strokeWidth={3} fillOpacity={1} fill="url(#colorPurchasers)" />
                                    </AreaChart>
                                </ResponsiveContainer>
                            </motion.div>
                        </div>

                        <div className="col-4 glass-card">
                            <h3 className="m-0 text-base font-semibold mb-3">Conversion Funnel</h3>
                            {hasWebGL ? (
                                <React.Suspense fallback={<div className="h-[450px] flex items-center justify-center opacity-50">Loading 3D Engine...</div>}>
                                    <ThreeDFunnel data={funnelData} />
                                </React.Suspense>
                            ) : (
                                <div className="h-[200px]">
                                    <PureCSSFunnel data={funnelData} />
                                </div>
                            )}
                            <div className="mt-4 pt-4 border-t border-white/10">
                                <div className="flex justify-between items-center mb-2">
                                    <h3 className="m-0 text-sm font-semibold">Viewers to Purchasers</h3>
                                    <span className="text-xl font-semibold gradient-text">
                                        {(latest.purchasers / latest.viewers * 100).toFixed(2)}%
                                    </span>
                                </div>
                                <div className="h-2 bg-white/5 rounded-full overflow-hidden shadow-inner flex items-center px-[2px]">
                                    <motion.div
                                        className="h-[4px] rounded-full shadow-[0_0_15px_rgba(99,110,250,0.5)]"
                                        initial={{ width: 0 }}
                                        animate={{ width: `${(latest.purchasers / latest.viewers * 100)}%` }}
                                        transition={{ type: 'spring', stiffness: 50, damping: 20 }}
                                        style={{ background: 'linear-gradient(90deg, #636efa, #00cc96)' }}
                                    />
                                </div>
                            </div>
                        </div>

                        {/* Bottom Row */}
                        <div className="col-12 glass-card" style={{ perspective: '1000px' }}>
                            <div className="flex justify-between items-start mb-3">
                                <div>
                                    <h3 className="m-0 text-base font-semibold">Conversion Rate Trends</h3>
                                    <p className="mt-1 text-xs opacity-50">Track how users progress through the funnel over time</p>
                                </div>
                                <div className="flex gap-6">
                                    <div className="flex items-center gap-2">
                                        <div className="w-3 h-3 rounded-full" style={{ background: '#00cc96' }}></div>
                                        <span className="text-xs opacity-70">Viewers → Cart</span>
                                    </div>
                                    <div className="flex items-center gap-2">
                                        <div className="w-3 h-3 rounded-full" style={{ background: '#ef4444' }}></div>
                                        <span className="text-xs opacity-70">Cart → Purchase</span>
                                    </div>
                                </div>
                            </div>
                            <motion.div
                                className="chart-container"
                                initial={{ rotateX: 5 }}
                                whileHover={{ rotateX: 0 }}
                                transition={{ type: 'spring', stiffness: 300 }}
                            >
                                <ResponsiveContainer width="100%" height="100%">
                                    <LineChart data={data}>
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
                                            tickFormatter={(val) => `${(val * 100).toFixed(0)}%`}
                                        />
                                        <Tooltip
                                            contentStyle={{ background: '#1a1d29', border: '1px solid rgba(255,255,255,0.1)', borderRadius: '12px' }}
                                            formatter={(val, name) => [`${(val * 100).toFixed(1)}%`, name]}
                                        />
                                        <Line
                                            type="monotone"
                                            dataKey="view_to_cart_rate"
                                            name="Viewers → Cart"
                                            stroke="#00cc96"
                                            strokeWidth={4}
                                            dot={{ fill: '#00cc96', r: 4 }}
                                            activeDot={{ r: 6 }}
                                        />
                                        <Line
                                            type="monotone"
                                            dataKey="cart_to_buy_rate"
                                            name="Cart → Purchase"
                                            stroke="#ef4444"
                                            strokeWidth={4}
                                            dot={{ fill: '#ef4444', r: 4 }}
                                            activeDot={{ r: 6 }}
                                        />
                                    </LineChart>
                                </ResponsiveContainer>
                            </motion.div>
                        </div>
                    </motion.div>
                ) : activeTab === 'clickstream' ? (
                    <motion.div
                        key="clickstream"
                        initial={{ opacity: 0, scale: 0.98 }}
                        animate={{ opacity: 1, scale: 1 }}
                        exit={{ opacity: 0, scale: 1.02 }}
                        className="dashboard-content-grid"
                    >
                        {/* Clickstream KPI Cards - Full Width Row */}
                        <div className="col-12" style={{ display: 'grid', gridTemplateColumns: 'repeat(6, 1fr)', gap: '0.75rem' }}>
                            <motion.div className={`glass-card ${flashCards.page_views ? 'flash-update' : ''}`} style={{ padding: '1.25rem 1rem', textAlign: 'center' }}>
                                <div className="text-xs opacity-70 mb-2">Page Views</div>
                                <div className="text-4xl font-bold" style={{ fontSize: '2.25rem', lineHeight: '1.2' }}>{clickstreamSummary.page_views.toLocaleString()}</div>
                                <div className="text-[10px] opacity-50 mt-1">per minute</div>
                            </motion.div>
                            <motion.div className={`glass-card ${flashCards.clicks ? 'flash-update' : ''}`} style={{ padding: '1.25rem 1rem', textAlign: 'center' }}>
                                <div className="text-xs opacity-70 mb-2">Clicks</div>
                                <div className="text-4xl font-bold" style={{ fontSize: '2.25rem', lineHeight: '1.2' }}>{clickstreamSummary.clicks.toLocaleString()}</div>
                                <div className="text-[10px] opacity-50 mt-1">per minute</div>
                            </motion.div>
                            <motion.div className={`glass-card ${flashCards.add_to_carts ? 'flash-update' : ''}`} style={{ padding: '1.25rem 1rem', textAlign: 'center' }}>
                                <div className="text-xs opacity-70 mb-2">Add to Cart</div>
                                <div className="text-4xl font-bold" style={{ fontSize: '2.25rem', lineHeight: '1.2' }}>{clickstreamSummary.add_to_carts.toLocaleString()}</div>
                                <div className="text-[10px] opacity-50 mt-1">per minute</div>
                            </motion.div>
                            <motion.div className={`glass-card ${flashCards.checkouts ? 'flash-update' : ''}`} style={{ padding: '1.25rem 1rem', textAlign: 'center' }}>
                                <div className="text-xs opacity-70 mb-2">Checkouts</div>
                                <div className="text-4xl font-bold" style={{ fontSize: '2.25rem', lineHeight: '1.2' }}>{clickstreamSummary.checkouts.toLocaleString()}</div>
                                <div className="text-[10px] opacity-50 mt-1">per minute</div>
                            </motion.div>
                            <motion.div className={`glass-card ${flashCards.purchases ? 'flash-update' : ''}`} style={{ padding: '1.25rem 1rem', textAlign: 'center' }}>
                                <div className="text-xs opacity-70 mb-2">Purchases</div>
                                <div className="text-4xl font-bold" style={{ fontSize: '2.25rem', lineHeight: '1.2' }}>{clickstreamSummary.purchases.toLocaleString()}</div>
                                <div className="text-[10px] opacity-50 mt-1">per minute</div>
                            </motion.div>
                            <motion.div className={`glass-card ${flashCards.revenue ? 'flash-update' : ''}`} style={{ padding: '1.25rem 1rem', textAlign: 'center' }}>
                                <div className="text-xs opacity-70 mb-2">Revenue</div>
                                <div className="text-4xl font-bold" style={{ fontSize: '2.25rem', lineHeight: '1.2' }}>${clickstreamSummary.revenue.toLocaleString(undefined, { maximumFractionDigits: 0 })}</div>
                                <div className="text-[10px] opacity-50 mt-1">per minute</div>
                            </motion.div>
                        </div>

                        {/* Event Timeline Chart */}
                        <div className="col-8 glass-card">
                            <div className="flex justify-between items-center mb-3">
                                <h3 className="m-0 text-base font-semibold">Event Timeline (Last 30 Minutes)</h3>
                                <div className="flex gap-4 text-xs">
                                    <span className="flex items-center gap-1"><div className="w-2 h-2 rounded-full bg-[#636efa]"></div> Page Views</span>
                                    <span className="flex items-center gap-1"><div className="w-2 h-2 rounded-full bg-[#00cc96]"></div> Clicks</span>
                                    <span className="flex items-center gap-1"><div className="w-2 h-2 rounded-full bg-[#f59e0b]"></div> Add to Cart</span>
                                    <span className="flex items-center gap-1"><div className="w-2 h-2 rounded-full bg-[#8b5cf6]"></div> Checkouts</span>
                                    <span className="flex items-center gap-1"><div className="w-2 h-2 rounded-full bg-[#ff6692]"></div> Purchases</span>
                                </div>
                            </div>
                            <div className="chart-container" style={{ height: '300px' }}>
                                <ResponsiveContainer width="100%" height="100%">
                                    <AreaChart data={clickstreamData}>
                                        <defs>
                                            <linearGradient id="colorPageViews" x1="0" y1="0" x2="0" y2="1">
                                                <stop offset="5%" stopColor="#636efa" stopOpacity={0.3} />
                                                <stop offset="95%" stopColor="#636efa" stopOpacity={0} />
                                            </linearGradient>
                                            <linearGradient id="colorClicks" x1="0" y1="0" x2="0" y2="1">
                                                <stop offset="5%" stopColor="#00cc96" stopOpacity={0.3} />
                                                <stop offset="95%" stopColor="#00cc96" stopOpacity={0} />
                                            </linearGradient>
                                            <linearGradient id="colorCart" x1="0" y1="0" x2="0" y2="1">
                                                <stop offset="5%" stopColor="#f59e0b" stopOpacity={0.3} />
                                                <stop offset="95%" stopColor="#f59e0b" stopOpacity={0} />
                                            </linearGradient>
                                            <linearGradient id="colorCheckout" x1="0" y1="0" x2="0" y2="1">
                                                <stop offset="5%" stopColor="#8b5cf6" stopOpacity={0.3} />
                                                <stop offset="95%" stopColor="#8b5cf6" stopOpacity={0} />
                                            </linearGradient>
                                            <linearGradient id="colorPurchase" x1="0" y1="0" x2="0" y2="1">
                                                <stop offset="5%" stopColor="#ff6692" stopOpacity={0.3} />
                                                <stop offset="95%" stopColor="#ff6692" stopOpacity={0} />
                                            </linearGradient>
                                        </defs>
                                        <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" vertical={false} />
                                        <XAxis
                                            dataKey="time_bucket"
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
                                        <Tooltip
                                            contentStyle={{ background: '#1a1d29', border: '1px solid rgba(255,255,255,0.1)', borderRadius: '12px' }}
                                            labelFormatter={(label) => new Date(label).toLocaleString()}
                                        />
                                        <Area type="monotone" dataKey="page_view" name="Page Views" stroke="#636efa" fill="url(#colorPageViews)" strokeWidth={2} />
                                        <Area type="monotone" dataKey="click" name="Clicks" stroke="#00cc96" fill="url(#colorClicks)" strokeWidth={2} />
                                        <Area type="monotone" dataKey="add_to_cart" name="Add to Cart" stroke="#f59e0b" fill="url(#colorCart)" strokeWidth={2} />
                                        <Area type="monotone" dataKey="checkout_start" name="Checkouts" stroke="#8b5cf6" fill="url(#colorCheckout)" strokeWidth={2} />
                                        <Area type="monotone" dataKey="purchase" name="Purchases" stroke="#ff6692" fill="url(#colorPurchase)" strokeWidth={2} />
                                    </AreaChart>
                                </ResponsiveContainer>
                            </div>
                        </div>

                        {/* Recent Sessions Table */}
                        <div className="col-4 glass-card">
                            <h3 className="m-0 text-base font-semibold mb-3">Recent Sessions</h3>
                            <div className="overflow-auto" style={{ maxHeight: '300px' }}>
                                <table className="w-full text-xs">
                                    <thead>
                                        <tr className="text-left opacity-50 border-b border-white/10">
                                            <th className="pb-2">Time</th>
                                            <th className="pb-2">City</th>
                                            <th className="pb-2">Events</th>
                                            <th className="pb-2 text-right">Revenue</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {recentSessions.map((session) => {
                                            const flashType = flashSessions[session.session_id];
                                            const flashClass = flashType === 'new' ? 'flash-row-new' : flashType === 'updated' ? 'flash-row-update' : '';
                                            return (
                                                <tr key={session.session_id} className={`border-b border-white/5 ${flashClass}`}>
                                                    <td className="py-2 opacity-70">{new Date(session.session_start).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false })}</td>
                                                    <td className="py-2">{session.geo_city}</td>
                                                    <td className="py-2">{session.event_count}</td>
                                                    <td className="py-2 text-right">${session.total_revenue.toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                                                </tr>
                                            );
                                        })}
                                        {recentSessions.length === 0 && (
                                            <tr><td colSpan="4" className="py-4 text-center opacity-50">No recent sessions</td></tr>
                                        )}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </motion.div>
                ) : (
                    <motion.div
                        key="producer"
                        initial={{ opacity: 0, scale: 0.98 }}
                        animate={{ opacity: 1, scale: 1 }}
                        exit={{ opacity: 0, scale: 1.02 }}
                        className="dashboard-content-grid"
                        style={{ flex: 1, display: 'grid' }}
                    >
                        <div className="col-4 glass-card" style={{ padding: '1rem' }}>
                            <div className="flex items-center justify-between mb-3">
                                <h3 className="m-0 text-base font-semibold">Producer</h3>
                                <div className={`w-2 h-2 rounded-full ${producerStatus.running ? 'bg-[#00cc96] animate-pulse' : 'bg-white/10'}`} />
                            </div>
                            <div className="flex flex-col gap-2">
                                <div className="flex gap-2 justify-between">
                                    <button
                                        onClick={startProducer}
                                        className="btn-primary !py-2 !text-[9px]"
                                        disabled={producerStatus.running}
                                    >
                                        Start
                                    </button>
                                    <button
                                        onClick={stopProducer}
                                        className="btn-secondary !py-2 !text-[9px]"
                                        disabled={!producerStatus.running}
                                    >
                                        Stop
                                    </button>
                                </div>
                            </div>
                        </div>

                        <div className="col-8 glass-card" style={{ padding: '1rem' }}>
                            <div className="flex justify-between items-center mb-3">
                                <h3 className="m-0 text-base font-semibold">Event Stream</h3>
                                <div className="px-2 py-0.5 rounded-full bg-white/5 border border-white/5">
                                    <span className="text-[9px] font-bold tracking-widest opacity-40 uppercase">Logs</span>
                                </div>
                            </div>

                            <div
                                ref={logContainerRef}
                                className="flex-1 bg-black/40 rounded-xl p-2 font-mono text-[9px] overflow-y-auto border border-white/5 shadow-inner"
                                style={{ paddingBottom: '6rem' }}
                            >
                                {producerStatus.output && producerStatus.output.length > 0 ? (
                                    <>
                                        {producerStatus.output.map((line, i) => {
                                            const isLast = i === producerStatus.output.length - 1;
                                            let colorClass = line.includes('[ERROR]') ? 'text-red-400' :
                                                line.includes('Producer started') ? 'text-green-400' :
                                                    'text-white/60';

                                            if (isLast) {
                                                return (
                                                    <div
                                                        key={i}
                                                        className="mb-4 px-2 py-1 rounded font-bold"
                                                        style={{
                                                            backgroundColor: 'rgba(0, 204, 150, 0.1)',
                                                            color: '#00cc96',
                                                            borderLeft: '2px solid #00cc96'
                                                        }}
                                                    >
                                                        <span className="opacity-30 mr-2">{'>'}</span>
                                                        {line}
                                                    </div>
                                                );
                                            }

                                            return (
                                                <div
                                                    key={i}
                                                    className={`mb-1.5 px-2 py-1 ${colorClass}`}
                                                >
                                                    <span className="opacity-30 mr-2">{'>'}</span>
                                                    {line}
                                                </div>
                                            );
                                        })}
                                        <div ref={logEndRef} />
                                    </>
                                ) : (
                                    <div className="text-white/20 italic p-2">No logs available. Start the producer to see activity...</div>
                                )}
                            </div>
                        </div>
                    </motion.div>
                )}
            </AnimatePresence>
        </div>
    );
};

const KPICard = ({ label, value, change, icon, gradient }) => (
    <motion.div
        className="col-3 glass-card"
        whileHover={{
            y: -4,
            rotateY: 3,
            rotateX: 3,
            boxShadow: '0 12px 30px rgba(0,0,0,0.5), 0 0 15px rgba(99, 110, 250, 0.2)'
        }}
        transition={{ type: 'spring', stiffness: 300 }}
    >
        <div className="flex justify-between items-start mb-1">
            <div className={`p-1.5 rounded-lg bg-white/5`}>
                {React.cloneElement(icon, { size: 16 })}
            </div>
            <div className={`trend ${change >= 0 ? 'trend-up' : 'trend-down'}`}>
                {change >= 0 ? <TrendingUp size={12} /> : <TrendingDown size={12} />}
                {Math.abs(change).toFixed(1)}%
            </div>
        </div>
        <div className="kpi-label">{label} </div>
        <div className="kpi-value">
            <AnimatePresence mode="wait">
                <motion.span
                    key={value}
                    initial={{ opacity: 0, y: 10 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, y: -10 }}
                >
                    {typeof value === 'number' ? value.toLocaleString() : value}
                </motion.span>
            </AnimatePresence>
        </div>
    </motion.div>
);

export default App;
