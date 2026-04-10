import { useState, useEffect, useRef } from 'react';
import { motion } from 'framer-motion';
import { Search, Database, Users, ShoppingCart, CreditCard, BarChart3, Calendar, Clock, Globe } from 'lucide-react';

const DateTimeInput = ({ value, onChange }) => {
    const dateInputRef = useRef(null);
    const timeInputRef = useRef(null);
    const [editValue, setEditValue] = useState('');
    const [isEditing, setIsEditing] = useState(false);

    const formatForDateInput = (val) => {
        if (!val) return '';
        return val.split('T')[0];
    };

    const formatForTimeInput = (val) => {
        if (!val) return '';
        const timePart = val.split('T')[1];
        return timePart ? timePart.slice(0, 5) : '';
    };

    const formatDisplay = (val) => {
        if (!val) return '';
        // val is in format YYYY-MM-DDTHH:MM (local time, NOT UTC)
        const [datePart, timePart] = val.split('T');
        if (!datePart) return '';
        const [year, month, day] = datePart.split('-');
        const time = timePart ? timePart.slice(0, 5) : '';
        return `${day}/${month}/${year} ${time}`;
    };

    const parseDisplay = (displayVal) => {
        // Parse DD/MM/YYYY HH:MM format
        const match = displayVal.match(/(\d{2})\/(\d{2})\/(\d{4})\s+(\d{2}):(\d{2})/);
        if (match) {
            const [, day, month, year, hours, minutes] = match;
            // Return in YYYY-MM-DDTHH:MM format (local time)
            return `${year}-${month}-${day}T${hours}:${minutes}`;
        }
        return null;
    };

    const handleDateChange = (e) => {
        const dateStr = e.target.value;
        if (dateStr) {
            const timePart = value ? value.split('T')[1] || '00:00' : '00:00';
            onChange(`${dateStr}T${timePart}`);
        }
    };

    const handleTimeChange = (e) => {
        const timeStr = e.target.value;
        if (timeStr) {
            const datePart = value ? value.split('T')[0] : new Date().toISOString().split('T')[0];
            onChange(`${datePart}T${timeStr}`);
        }
    };

    const openDatePicker = () => {
        if (dateInputRef.current) {
            dateInputRef.current.showPicker?.() || dateInputRef.current.click();
        }
    };

    const openTimePicker = () => {
        if (timeInputRef.current) {
            timeInputRef.current.showPicker?.() || timeInputRef.current.click();
        }
    };

    const handleInputChange = (e) => {
        setEditValue(e.target.value);
    };

    const handleBlur = () => {
        const parsed = parseDisplay(editValue);
        if (parsed) {
            onChange(parsed);
        }
        setIsEditing(false);
    };

    const handleFocus = () => {
        setEditValue(formatDisplay(value));
        setIsEditing(true);
    };

    const handleKeyDown = (e) => {
        if (e.key === 'Enter') {
            const parsed = parseDisplay(editValue);
            if (parsed) {
                onChange(parsed);
            }
            setIsEditing(false);
            e.target.blur();
        }
    };

    return (
        <div style={{ display: 'flex', alignItems: 'center' }}>
            <input
                ref={dateInputRef}
                type="date"
                value={formatForDateInput(value)}
                onChange={handleDateChange}
                style={{
                    position: 'absolute',
                    opacity: 0,
                    width: '1px',
                    height: '1px',
                    pointerEvents: 'none'
                }}
            />
            <input
                ref={timeInputRef}
                type="time"
                value={formatForTimeInput(value)}
                onChange={handleTimeChange}
                style={{
                    position: 'absolute',
                    opacity: 0,
                    width: '1px',
                    height: '1px',
                    pointerEvents: 'none'
                }}
            />
            <input
                type="text"
                value={isEditing ? editValue : formatDisplay(value)}
                onChange={handleInputChange}
                onFocus={handleFocus}
                onBlur={handleBlur}
                onKeyDown={handleKeyDown}
                placeholder="DD/MM/YYYY HH:MM"
                style={{
                    height: '38px',
                    padding: '0 0.75rem',
                    borderRadius: '0.5rem 0 0 0.5rem',
                    background: 'rgba(0, 0, 0, 0.3)',
                    border: '1px solid rgba(255, 255, 255, 0.1)',
                    borderRight: 'none',
                    color: 'rgba(255, 255, 255, 0.9)',
                    fontSize: '0.875rem',
                    minWidth: '160px',
                    outline: 'none'
                }}
            />
            <motion.button
                onClick={openDatePicker}
                whileHover={{ background: 'rgba(255, 255, 255, 0.1)' }}
                whileTap={{ scale: 0.95 }}
                style={{
                    height: '38px',
                    width: '38px',
                    background: 'rgba(0, 0, 0, 0.3)',
                    border: '1px solid rgba(255, 255, 255, 0.1)',
                    borderLeft: 'none',
                    borderRight: 'none',
                    color: 'rgba(255, 255, 255, 0.5)',
                    cursor: 'pointer',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center'
                }}
            >
                <Calendar size={16} />
            </motion.button>
            <motion.button
                onClick={openTimePicker}
                whileHover={{ background: 'rgba(255, 255, 255, 0.1)' }}
                whileTap={{ scale: 0.95 }}
                style={{
                    height: '38px',
                    width: '38px',
                    borderRadius: '0 0.5rem 0.5rem 0',
                    background: 'rgba(0, 0, 0, 0.3)',
                    border: '1px solid rgba(255, 255, 255, 0.1)',
                    borderLeft: 'none',
                    color: 'rgba(255, 255, 255, 0.5)',
                    cursor: 'pointer',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center'
                }}
            >
                <Clock size={16} />
            </motion.button>
        </div>
    );
};

const KPICard = ({ label, value, icon: Icon, color }) => (
    <motion.div
        style={{
            background: 'rgba(255, 255, 255, 0.03)',
            backdropFilter: 'blur(10px)',
            borderRadius: '0.75rem',
            padding: '1rem',
            border: '1px solid rgba(255, 255, 255, 0.08)',
            minWidth: '100px',
            position: 'relative',
            overflow: 'hidden'
        }}
        whileHover={{
            borderColor: 'rgba(255, 255, 255, 0.15)',
            transition: { duration: 0.2 }
        }}
    >
        <div style={{
            position: 'absolute',
            top: 0,
            left: 0,
            right: 0,
            height: '2px',
            background: color
        }} />
        <Icon size={20} style={{ color: color, marginBottom: '0.5rem' }} />
        <div style={{
            fontSize: '0.65rem',
            color: 'rgba(255, 255, 255, 0.4)',
            textTransform: 'uppercase',
            letterSpacing: '0.05em',
            marginBottom: '0.25rem'
        }}>
            {label}
        </div>
        <div style={{
            fontSize: '1.5rem',
            fontWeight: 600,
            color: 'rgba(255, 255, 255, 0.9)'
        }}>
            {value.toLocaleString()}
        </div>
    </motion.div>
);

const QueriesTab = () => {
    const today = new Date();
    const todayStart = new Date(today);
    todayStart.setHours(0, 0, 0, 0);
    const todayEnd = new Date(today);
    todayEnd.setHours(23, 59, 0, 0);

    const formatForInput = (date) => {
        // Format as YYYY-MM-DDTHH:MM in local time (not UTC)
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        const hours = String(date.getHours()).padStart(2, '0');
        const minutes = String(date.getMinutes()).padStart(2, '0');
        return `${year}-${month}-${day}T${hours}:${minutes}`;
    };

    // Convert local datetime string to UTC ISO string for API
    const localToUTC = (localStr) => {
        if (!localStr) return '';
        const [datePart, timePart] = localStr.split('T');
        if (!datePart || !timePart) return '';
        const [year, month, day] = datePart.split('-').map(Number);
        const [hours, minutes] = timePart.split(':').map(Number);
        // Create date in local timezone and convert to UTC
        const localDate = new Date(year, month - 1, day, hours, minutes);
        return localDate.toISOString().slice(0, 19); // Include seconds
    };

    const [startTime, setStartTime] = useState(formatForInput(todayStart));
    const [endTime, setEndTime] = useState(formatForInput(todayEnd));
    const [results, setResults] = useState([]);
    const [aggregates, setAggregates] = useState({
        total_viewers: 0,
        total_carters: 0,
        total_purchasers: 0,
        record_count: 0,
        country_count: 0
    });
    const [loading, setLoading] = useState(false);
    const [hasQueried, setHasQueried] = useState(false);

    const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

    const runQuery = async () => {
        setLoading(true);
        try {
            // Convert local times to UTC for API
            const startTs = localToUTC(startTime);
            const endTs = localToUTC(endTime);

            const [detailRes, aggRes] = await Promise.all([
                fetch(`${API_URL}/api/query/funnel?start_time=${encodeURIComponent(startTs)}&end_time=${encodeURIComponent(endTs)}`),
                fetch(`${API_URL}/api/query/funnel/aggregate?start_time=${encodeURIComponent(startTs)}&end_time=${encodeURIComponent(endTs)}`)
            ]);

            if (!detailRes.ok) throw new Error(`Detail query failed: ${detailRes.statusText}`);
            if (!aggRes.ok) throw new Error(`Aggregate query failed: ${aggRes.statusText}`);

            const detailData = await detailRes.json();
            const aggData = await aggRes.json();

            setResults(detailData.data || []);
            setAggregates(aggData.data || {
                total_viewers: 0,
                total_carters: 0,
                total_purchasers: 0,
                record_count: 0,
                country_count: 0
            });
            setHasQueried(true);
        } catch (err) {
            console.error(err);
        } finally {
            setLoading(false);
        }
    };

    const formatTimestamp = (ts) => {
        if (!ts) return '-';
        try {
            const date = new Date(ts);
            const day = date.getDate().toString().padStart(2, '0');
            const month = (date.getMonth() + 1).toString().padStart(2, '0');
            const year = date.getFullYear();
            const hours = date.getHours().toString().padStart(2, '0');
            const minutes = date.getMinutes().toString().padStart(2, '0');
            return `${day}/${month}/${year} ${hours}:${minutes}`;
        } catch {
            return ts;
        }
    };

    return (
        <div style={{ padding: '1.5rem' }}>
            {/* Header */}
            <motion.div
                initial={{ opacity: 0, y: -10 }}
                animate={{ opacity: 1, y: 0 }}
                style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.5rem',
                    marginBottom: '1.5rem',
                    color: 'rgba(255, 255, 255, 0.9)',
                    fontSize: '1.1rem',
                    fontWeight: 600
                }}
            >
                <Globe size={20} />
                <span>Query Funnel Data by Country</span>
            </motion.div>

            {/* Time Range Selector Card */}
            <motion.div
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.1 }}
                style={{
                    background: 'rgba(255, 255, 255, 0.03)',
                    backdropFilter: 'blur(10px)',
                    borderRadius: '1rem',
                    padding: '1.5rem',
                    border: '1px solid rgba(255, 255, 255, 0.08)',
                    marginBottom: '1.5rem'
                }}
            >
                <div style={{ display: 'flex', alignItems: 'flex-end', gap: '1rem', flexWrap: 'wrap' }}>
                    <div>
                        <label style={{
                            display: 'block',
                            fontSize: '0.75rem',
                            color: 'rgba(255, 255, 255, 0.5)',
                            textTransform: 'uppercase',
                            letterSpacing: '0.05em',
                            marginBottom: '0.5rem'
                        }}>
                            Start Time
                        </label>
                        <DateTimeInput value={startTime} onChange={setStartTime} />
                    </div>

                    <div>
                        <label style={{
                            display: 'block',
                            fontSize: '0.75rem',
                            color: 'rgba(255, 255, 255, 0.5)',
                            textTransform: 'uppercase',
                            letterSpacing: '0.05em',
                            marginBottom: '0.5rem'
                        }}>
                            End Time
                        </label>
                        <DateTimeInput value={endTime} onChange={setEndTime} />
                    </div>

                    <motion.button
                        onClick={runQuery}
                        disabled={loading}
                        whileHover={{ scale: 1.02 }}
                        whileTap={{ scale: 0.98 }}
                        style={{
                            height: '38px',
                            padding: '0 1.25rem',
                            borderRadius: '0.5rem',
                            background: 'linear-gradient(135deg, #6366F1 0%, #8B5CF6 50%, #06B6D4 100%)',
                            border: 'none',
                            color: 'white',
                            fontWeight: 500,
                            fontSize: '0.875rem',
                            cursor: loading ? 'not-allowed' : 'pointer',
                            opacity: loading ? 0.7 : 1,
                            display: 'flex',
                            alignItems: 'center',
                            gap: '0.5rem',
                            marginLeft: '0.5rem'
                        }}
                    >
                        <Search size={16} />
                        Run Query
                    </motion.button>
                </div>
            </motion.div>

            {/* KPI Cards */}
            <motion.div
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.2 }}
                style={{
                    display: 'flex',
                    gap: '1rem',
                    marginBottom: '1.5rem',
                    flexWrap: 'wrap'
                }}
            >
                <KPICard
                        label="Total Viewers"
                        value={aggregates.total_viewers || 0}
                        icon={Users}
                        color="#636efa"
                    />
                    <KPICard
                        label="Total Carters"
                        value={aggregates.total_carters || 0}
                        icon={ShoppingCart}
                        color="#00cc96"
                    />
                    <KPICard
                        label="Total Purchasers"
                        value={aggregates.total_purchasers || 0}
                        icon={CreditCard}
                        color="#ff6692"
                    />
                    <KPICard
                        label="Records Found"
                        value={aggregates.record_count || 0}
                        icon={BarChart3}
                        color="#8B5CF6"
                    />
                    <KPICard
                        label="Countries"
                        value={aggregates.country_count || 0}
                        icon={Globe}
                        color="#F59E0B"
                    />
            </motion.div>

            {/* Results Table */}
            <motion.div
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.3 }}
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
                        <Globe size={18} />
                        <span>Records by Country</span>
                    </div>
                    {hasQueried && results.length > 0 && (
                        <span style={{
                            fontSize: '0.875rem',
                            color: 'rgba(255, 255, 255, 0.5)'
                        }}>
                            {results.length} records
                        </span>
                    )}
                </div>

                {!hasQueried || results.length === 0 ? (
                    <div style={{
                        padding: '4rem',
                        textAlign: 'center',
                        color: 'rgba(255, 255, 255, 0.5)'
                    }}>
                        <Database size={48} style={{ marginBottom: '1rem', opacity: 0.3 }} />
                        <p style={{ marginBottom: '0.5rem' }}>
                            {hasQueried ? 'No records found for the selected time range' : 'No records to display'}
                        </p>
                        <p style={{ fontSize: '0.875rem', opacity: 0.6 }}>
                            Select a time range and click "Run Query" to fetch data
                        </p>
                    </div>
                ) : (
                    <div style={{ overflowX: 'auto' }}>
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
                                    }}>Window Start</th>
                                    <th style={{
                                        padding: '0.875rem 1.5rem',
                                        textAlign: 'left',
                                        fontSize: '0.75rem',
                                        color: '#F59E0B',
                                        textTransform: 'uppercase',
                                        letterSpacing: '0.05em',
                                        fontWeight: 500
                                    }}>Country</th>
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
                                </tr>
                            </thead>
                            <tbody>
                                {results.map((row, idx) => (
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
                                            {formatTimestamp(row.window_start)}
                                        </td>
                                        <td style={{
                                            padding: '0.875rem 1.5rem',
                                            fontSize: '0.875rem',
                                            color: '#F59E0B'
                                        }}>
                                            {row.country_name || row.country || '-'}
                                        </td>
                                        <td style={{
                                            padding: '0.875rem 1.5rem',
                                            fontSize: '0.875rem',
                                            color: '#636efa',
                                            textAlign: 'right',
                                            fontWeight: 500
                                        }}>
                                            {row.viewers?.toLocaleString() || 0}
                                        </td>
                                        <td style={{
                                            padding: '0.875rem 1.5rem',
                                            fontSize: '0.875rem',
                                            color: '#00cc96',
                                            textAlign: 'right',
                                            fontWeight: 500
                                        }}>
                                            {row.carters?.toLocaleString() || 0}
                                        </td>
                                        <td style={{
                                            padding: '0.875rem 1.5rem',
                                            fontSize: '0.875rem',
                                            color: '#ff6692',
                                            textAlign: 'right',
                                            fontWeight: 500
                                        }}>
                                            {row.purchasers?.toLocaleString() || 0}
                                        </td>
                                    </motion.tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                )}
            </motion.div>
        </div>
    );
};

export default QueriesTab;
