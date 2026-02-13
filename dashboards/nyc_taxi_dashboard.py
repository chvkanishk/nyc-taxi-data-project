# dashboards/nyc_taxi_dashboard.py
"""
NYC Taxi Interactive Dashboard
Built with Plotly Dash
FIXED VERSION - Includes Delta Lake configuration
"""

import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from pyspark.sql import SparkSession
import os
import sys

# ===== WINDOWS HADOOP FIX =====
os.environ['HADOOP_HOME'] = r'C:\hadoop'
if r'C:\hadoop\bin' not in sys.path:
    sys.path.insert(0, r'C:\hadoop\bin')
# ==============================

# Initialize Spark with Delta Lake configuration
print("🚀 Starting Spark session...")
spark = SparkSession.builder \
    .appName("Dashboard") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .master("local[*]") \
    .getOrCreate()

print("✅ Spark session started!")

# Load data
print("📊 Loading data from Gold layer...")

try:
    daily_metrics = spark.read.format("delta").load("data/processed/gold/daily_metrics").toPandas()
    print(f"   ✅ Daily metrics loaded: {len(daily_metrics)} rows")
except Exception as e:
    print(f"   ❌ Error loading daily_metrics: {e}")
    daily_metrics = pd.DataFrame()

try:
    hourly_demand = spark.read.format("delta").load("data/processed/gold/hourly_demand").toPandas()
    print(f"   ✅ Hourly demand loaded: {len(hourly_demand)} rows")
except Exception as e:
    print(f"   ❌ Error loading hourly_demand: {e}")
    hourly_demand = pd.DataFrame()

try:
    zone_analytics = spark.read.format("delta").load("data/processed/gold/zone_analytics").toPandas()
    print(f"   ✅ Zone analytics loaded: {len(zone_analytics)} rows")
except Exception as e:
    print(f"   ❌ Error loading zone_analytics: {e}")
    zone_analytics = pd.DataFrame()

try:
    payment_analysis = spark.read.format("delta").load("data/processed/gold/payment_analysis").toPandas()
    print(f"   ✅ Payment analysis loaded: {len(payment_analysis)} rows")
except Exception as e:
    print(f"   ❌ Error loading payment_analysis: {e}")
    payment_analysis = pd.DataFrame()

spark.stop()
print("✅ All data loaded successfully!\n")

# Check if data was loaded
if daily_metrics.empty:
    print("⚠️  WARNING: No data loaded. Did you run gold_layer.py first?")
    print("   Run: python src/transformation/gold_layer.py")
    exit(1)

# Create month labels
daily_metrics['month'] = daily_metrics['pickup_year'].astype(str) + '-' + daily_metrics['pickup_month'].astype(str).str.zfill(2)

# Calculate KPIs
total_trips = daily_metrics['total_trips'].sum()
total_revenue = daily_metrics['total_revenue'].sum()
avg_distance = daily_metrics['avg_trip_distance'].mean()
avg_fare = daily_metrics['total_fare'].sum() / daily_metrics['total_trips'].sum() if daily_metrics['total_trips'].sum() > 0 else 0

# Initialize Dash app
app = dash.Dash(__name__)

# Define layout
app.layout = html.Div([
    html.H1("🚕 NYC Taxi Data Analytics Dashboard", 
            style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': 30, 'fontFamily': 'Arial'}),
    
    # KPI Cards
    html.Div([
        html.Div([
            html.H3("Total Trips", style={'margin': '0', 'color': '#7f8c8d', 'fontSize': '14px'}),
            html.H2(f"{total_trips:,.0f}", 
                   style={'color': '#3498db', 'margin': '10px 0 0 0', 'fontSize': '32px'})
        ], className='kpi-card', style={
            'background': 'white',
            'padding': '20px',
            'borderRadius': '10px',
            'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
            'textAlign': 'center',
            'width': '22%'
        }),
        
        html.Div([
            html.H3("Total Revenue", style={'margin': '0', 'color': '#7f8c8d', 'fontSize': '14px'}),
            html.H2(f"${total_revenue:,.2f}", 
                   style={'color': '#2ecc71', 'margin': '10px 0 0 0', 'fontSize': '32px'})
        ], className='kpi-card', style={
            'background': 'white',
            'padding': '20px',
            'borderRadius': '10px',
            'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
            'textAlign': 'center',
            'width': '22%'
        }),
        
        html.Div([
            html.H3("Avg Trip Distance", style={'margin': '0', 'color': '#7f8c8d', 'fontSize': '14px'}),
            html.H2(f"{avg_distance:.2f} mi", 
                   style={'color': '#e74c3c', 'margin': '10px 0 0 0', 'fontSize': '32px'})
        ], className='kpi-card', style={
            'background': 'white',
            'padding': '20px',
            'borderRadius': '10px',
            'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
            'textAlign': 'center',
            'width': '22%'
        }),
        
        html.Div([
            html.H3("Avg Fare", style={'margin': '0', 'color': '#7f8c8d', 'fontSize': '14px'}),
            html.H2(f"${avg_fare:.2f}", 
                   style={'color': '#f39c12', 'margin': '10px 0 0 0', 'fontSize': '32px'})
        ], className='kpi-card', style={
            'background': 'white',
            'padding': '20px',
            'borderRadius': '10px',
            'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
            'textAlign': 'center',
            'width': '22%'
        }),
    ], style={'display': 'flex', 'justifyContent': 'space-around', 'marginBottom': 40}),
    
    # Revenue Trend
    html.Div([
        html.H2("📊 Revenue Trend Over Time", style={'color': '#2c3e50'}),
        dcc.Graph(
            figure=px.line(
                daily_metrics.groupby('month')['total_revenue'].sum().reset_index(),
                x='month',
                y='total_revenue',
                title='Monthly Revenue',
                labels={'total_revenue': 'Revenue ($)', 'month': 'Month'}
            ).update_traces(line_color='#3498db', line_width=3)
            .update_layout(
                plot_bgcolor='white',
                paper_bgcolor='white',
                font={'family': 'Arial'}
            )
        )
    ], style={'marginBottom': 40}),
    
    # Hourly Demand
    html.Div([
        html.H2("⏰ Hourly Demand Pattern", style={'color': '#2c3e50'}),
        dcc.Graph(
            figure=px.bar(
                hourly_demand,
                x='pickup_hour',
                y='total_trips',
                color='is_weekend',
                barmode='group',
                title='Trips by Hour (Weekday vs Weekend)',
                labels={'total_trips': 'Number of Trips', 'pickup_hour': 'Hour of Day', 'is_weekend': 'Weekend'}
            ).update_layout(
                plot_bgcolor='white',
                paper_bgcolor='white',
                font={'family': 'Arial'}
            )
        )
    ], style={'marginBottom': 40}),
    
    # Top Zones
    html.Div([
        html.H2("🗺️ Top 15 Busiest Zones", style={'color': '#2c3e50'}),
        dcc.Graph(
            figure=px.bar(
                zone_analytics.nlargest(15, 'zone_activity_score'),
                x='location_id',
                y='zone_activity_score',
                title='Busiest Taxi Zones',
                labels={'zone_activity_score': 'Total Pickups + Dropoffs', 'location_id': 'Zone ID'}
            ).update_traces(marker_color='#2ecc71')
            .update_layout(
                plot_bgcolor='white',
                paper_bgcolor='white',
                font={'family': 'Arial'}
            )
        )
    ], style={'marginBottom': 40}),
    
    # Payment Analysis
    html.Div([
        html.Div([
            html.H2("💳 Payment Distribution", style={'color': '#2c3e50'}),
            dcc.Graph(
                figure=px.pie(
                    payment_analysis,
                    values='total_trips',
                    names='payment_type',
                    title='Trips by Payment Type'
                ).update_layout(
                    paper_bgcolor='white',
                    font={'family': 'Arial'}
                )
            )
        ], style={'width': '48%', 'display': 'inline-block'}),
        
        html.Div([
            html.H2("💰 Tip Analysis", style={'color': '#2c3e50'}),
            dcc.Graph(
                figure=px.bar(
                    payment_analysis,
                    x='payment_type',
                    y='avg_tip_percentage',
                    title='Average Tip % by Payment Type',
                    labels={'avg_tip_percentage': 'Avg Tip %', 'payment_type': 'Payment Type'}
                ).update_traces(marker_color='#f39c12')
                .update_layout(
                    plot_bgcolor='white',
                    paper_bgcolor='white',
                    font={'family': 'Arial'}
                )
            )
        ], style={'width': '48%', 'display': 'inline-block', 'float': 'right'})
    ], style={'marginBottom': 40}),
    
    # Footer
    html.Div([
        html.Hr(),
        html.P("Built with Plotly Dash | Data: NYC TLC", 
               style={'textAlign': 'center', 'color': '#7f8c8d'})
    ])
    
], style={'padding': 40, 'fontFamily': 'Arial, sans-serif', 'backgroundColor': '#ecf0f1'})


if __name__ == '__main__':
    print("\n" + "="*60)
    print("🚀 NYC Taxi Dashboard Starting...")
    print("="*60)
    print(f"📊 Data loaded:")
    print(f"   - Daily metrics: {len(daily_metrics)} rows")
    print(f"   - Hourly demand: {len(hourly_demand)} rows")
    print(f"   - Zone analytics: {len(zone_analytics)} rows")
    print(f"   - Payment analysis: {len(payment_analysis)} rows")
    print("\n🌐 Dashboard URL: http://127.0.0.1:8050")
    print("⏹️  Press Ctrl+C to stop\n")
    print("="*60 + "\n")
    
    app.run(debug=True, port=8050)