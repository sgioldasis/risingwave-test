# ML Predictions Demo Script - Script Runner Guide

This guide shows exactly what to click in the Script Runner for a live ML predictions demo using scikit-learn.

## Quick Demo (3 minutes)

### Step 1: Start Services (if not running)
**Click:** `🚀 Start Services` ([`1_up.sh`](bin/1_up.sh:1))
- Wait for all containers to start
- Look for green checkmarks in the output
- Takes ~30-60 seconds

### Step 2: Run dbt Models
**Click:** `📊 Run dbt` ([`3_run_dbt.sh`](bin/3_run_dbt.sh:1))
- Creates the `funnel_training` materialized view
- Takes ~30 seconds

### Step 3: Generate Data
**Click:** `🚀 Start Producer` ([`3_run_producer.sh`](bin/3_run_producer.sh:1))
- Let it run for 3-5 minutes to generate enough data
- The ML models need at least 2-3 minutes of data to train

### Step 4: Start Dashboard
**Click:** `✨ Run Modern Dashboard` ([`4_run_modern.sh`](bin/4_run_modern.sh:1))
- Opens the dashboard at http://localhost:5173
- Click on **"Predictions"** tab
- The models will auto-train on startup and show predictions!

---

## Manual Demo (More Control)

If you want to explain each step:

### Step 1: Start Services
**Click:** `🚀 Start Services`

### Step 2: Run dbt
**Click:** `📊 Run dbt`
- Creates the `funnel_training` materialized view

### Step 3: Generate Data
**Click:** `🚀 Start Producer`
- Let it run for 3-5 minutes (generates enough data)
- Then stop it

### Step 4: Start Dashboard
**Click:** `✨ Run Modern Dashboard`
- Navigate to Predictions tab
- Models auto-train using scikit-learn

---

## Demo Talking Points by Tab

### Dashboard Tab
> "Here's our real-time funnel - page views, cart additions, and purchases streaming through RisingWave from Kafka. Every minute we get updated metrics."

### Producer Tab
> "This shows our event producer running - it generates synthetic user activity at configurable rates."

### Predictions Tab (NEW!)
> "Now here's where it gets interesting. Using scikit-learn, we've trained ML models on the last 5 minutes of data to predict what will happen in the next minute.

> See these cards? They're showing our predictions for the next minute's viewers, carters, and purchasers.

> The chart shows historical data with a dashed line for our prediction. When the next minute's actual data arrives, we can compare how accurate we were!

> The models are continuously learning - as more data flows through RisingWave, the predictions get better."

---

## What to Show

1. **Show the prediction cards** - Point out the predicted values and % changes
2. **Show the comparison chart** - Explain the dashed prediction line
3. **Show model status** - All 5 models training in real-time using scikit-learn
4. **Wait for next minute** - Show actual vs predicted comparison

---

## Troubleshooting During Demo

### If predictions show "N/A"
- Models may still be training
- Wait 30 seconds and refresh
- Ensure producer has generated at least 2-3 minutes of data

### If dashboard won't load
- Check `✨ Run Modern Dashboard` is clicked
- Try refreshing the browser

---

## Script Order Reference

| Order | Script | When to Click | Duration |
|-------|--------|---------------|----------|
| 1 | 🚀 Start Services | First | 1 min |
| 2 | 📊 Run dbt | After services | 30 sec |
| 3 | 🚀 Start Producer | After dbt | 3-5 min |
| 4 | ✨ Run Modern Dashboard | After data | Ongoing |
