import dash
from dash import dcc, html, Input, Output
import dash_mantine_components as dmc
from dash_iconify import DashIconify
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime

# Import the connector we built
from connector import DataConnector

app = dash.Dash(__name__)

# --- STYLING CONSTANTS ---
MODE_COLORS = {
    "bus": "#38bdf8",   # Sky Blue
    "tram": "#10b981",  # Emerald Green
    "metro": "#fbbf24", # Amber
    "train": "#fbbf24", 
    "all": "#818cf8"    # Indigo
}

# --- UI HELPER COMPONENTS ---

def create_kpi_card(label, value, icon, color, id_val):
    return dmc.Paper(
        withBorder=True, p="md", radius="md",
        children=[
            dmc.Group(justify="space-between", children=[
                dmc.Text(label, size="xs", c="dimmed", fw=700),
                DashIconify(icon=icon, width=20, color=color),
            ]),
            dmc.Text(value, id=id_val, size="xl", fw=700, mt="sm"),
        ],
    )

def create_route_leaderboard(routes):
    if not routes:
        return dmc.Text("No route data available", c="dimmed", size="sm", ta="center", mt="md")

    rows = []
    for r in routes:
        otp_val = float(str(r['otp_pct']).replace("%", ""))
        status_color = "red" if otp_val < 70 else "green"

        rows.append(
            html.Tr([
                html.Td([
                    dmc.Group(justify="space-between", align="flex-start", wrap="wrap", children=[
                        # Removed truncate and maxWidth to allow wrapping
                        dmc.Text(r['name'], size="xs", fw=600, style={"flex": 1, "wordBreak": "break-word"}),
                        dmc.Group(gap="xs", children=[
                             dmc.Text(f"{otp_val}%", size="xs", fw=700, c=status_color),
                             dmc.Badge(r['mode'].upper(), size="xs", variant="outline")
                        ])
                    ])
                ], style={"padding": "10px 0px"}) # Slightly more padding for multi-line comfort
            ])
        )
    return dmc.Table(children=[html.Tbody(rows)], withRowBorders=True)

def create_stop_leaderboard(stops):
    if not stops:
        return dmc.Text("No stop data available", c="dimmed", size="sm", ta="center", mt="md")

    rows = []
    for s in stops:
        rows.append(
            html.Tr([
                html.Td([
                    dmc.Group(justify="space-between", align="flex-start", wrap="wrap", children=[
                        # Allowing text to grow and wrap
                        dmc.Text(s['name'], size="xs", style={"flex": 1, "wordBreak": "break-word"}),
                        dmc.Text(
                            f"{s['visit_count']} visits", 
                            size="xs", 
                            c="dimmed", 
                            fw=500,
                            style={"whiteSpace": "nowrap"} # Keeps "X visits" on one line
                        )
                    ])
                ], style={"padding": "10px 0px"})
            ])
        )
    return dmc.Table(children=[html.Tbody(rows)], withRowBorders=True)

# --- APP LAYOUT ---

app.layout = dmc.MantineProvider(
    forceColorScheme="dark",
    children=dmc.Container(
        size="xl", p="xl",
        children=[
            # Header
            dmc.Group(justify="space-between", mb="xl", children=[
                dmc.Stack(gap=0, children=[
                    dmc.Title("Transit Reliability Monitor", order=1),
                    dmc.Text("Real-time GTFS Pipeline", c="dimmed", size="sm")
                ]),
                dmc.Badge(id="live-status", color="blue", variant="light", size="lg")
            ]),

            # KPI Row
            dmc.SimpleGrid(cols={"base": 1, "sm": 3}, spacing="md", mb="xl", children=[
                create_kpi_card("Active Vehicles", "0", "lucide:bus", MODE_COLORS["bus"], "kpi-active"),
                create_kpi_card("Data freshness", "0%", "lucide:activity", MODE_COLORS["tram"], "kpi-latency-text"),
                create_kpi_card("Scheduled vehicles", "0", "lucide:map", MODE_COLORS["all"], "kpi-routes-count"),
            ]),

            # Main Grid
            dmc.Grid(gutter="md", children=[
                # Map Column
                dmc.GridCol(span={"base": 12, "lg": 7}, children=[
                    dmc.Paper(
                        withBorder=True, radius="md", style={"height": "650px", "overflow": "hidden"},
                        children=dcc.Graph(id="live-map", style={"height": "100%"}, config={'displayModeBar': False})
                    ),
                    dmc.Paper(withBorder=True, p="md", radius="md", children=[
                        dmc.Text("24h Delay Trend (Minutes)", fw=600, mb="xs"),
                        dcc.Graph(id="delay-trend-chart", style={"height": "200px"}, config={'displayModeBar': False})
                    ]),
                ]),
                
                # Sidebar Column
                dmc.GridCol(span={"base": 12, "lg": 5}, children=[
                    dmc.Stack(gap="xs", children=[
                        # Filter Control
                        dmc.Paper(withBorder=True, p="xs", radius="md", children=[
                            dmc.SegmentedControl(
                                id="mode-filter",
                                fullWidth=True,
                                radius="md",
                                value="all",
                                data=[
                                    {"label": "All", "value": "all"},
                                    {"label": "Bus", "value": "bus"},
                                    {"label": "Tram", "value": "tram"},
                                    {"label": "Train", "value": "train"},
                                ],
                                color="blue",
                            ),
                        ]),

                        # OTP Gauge
                        dmc.Paper(withBorder=True, p="sm", radius="md", children=[
                            dmc.Text("Performance Gauge", fw=600, mb="xs"),
                            dcc.Graph(id="otp-gauge", style={"height": "160px"}, config={'displayModeBar': False})
                        ]),

                        # Routes
                        dmc.Paper(withBorder=True, p="md", radius="md", children=[
                            dmc.Group(justify="space-between", mb="xs", children=[
                                dmc.Text("Top Problem Routes", fw=600),
                                dmc.Badge("Low OTP", size="xs", color="orange")
                            ]),
                            html.Div(id="problem-routes-list")
                        ]),

                        # Stops
                        dmc.Paper(withBorder=True, p="md", radius="md", children=[
                            dmc.Text("Most Visited Stops", fw=600, mb="xs"),
                            html.Div(id="problem-stops-list")
                        ])
                    ])
                ])
            ]),
            dcc.Interval(id='timer', interval=5000) # Refresh every 5 seconds
        ]
    )
)

# --- CALLBACKS ---

@app.callback(
    [Output('live-map', 'figure'),
     Output('delay-trend-chart', 'figure'),
     Output('otp-gauge', 'figure'),
     Output('problem-routes-list', 'children'),
     Output('problem-stops-list', 'children'),
     Output('live-status', 'children'),
     Output('kpi-active', 'children'),
     Output('kpi-latency-text', 'children'),
     Output('kpi-routes-count', 'children')],
    [Input('timer', 'n_intervals'),
     Input('mode-filter', 'value')]
)
def update_dashboard(n, selected_mode):
    query_mode = None if selected_mode == "all" else selected_mode

    # 1. DATA FETCHING
    df_live = DataConnector.get_live_locations(mode=query_mode)
    df_delay = DataConnector.get_delay_trend()
    otp_val = DataConnector.get_otp_score(mode=query_mode)
    routes = DataConnector.get_problem_routes(mode=query_mode)
    stops = DataConnector.get_most_popular_stops(mode=query_mode)

    # 2. MAP LOGIC
    map_fig = go.Figure()

    if not df_live.empty:
        # Group by mode to give different colors
        for mode_type, group in df_live.groupby('mode'):
            map_fig.add_trace(go.Scattermapbox(
                lat=group['lat'],
                lon=group['lon'],
                mode='markers',
                name=mode_type.upper(),
                marker=go.scattermapbox.Marker(
                    size=10,
                    color=MODE_COLORS.get(mode_type, "#ffffff"),
                    opacity=0.8
                ),
                hoverinfo="text",
                hovertext=group['route_name'] + " to " + group['headsign']
            ))
    
    map_fig.update_layout(
        mapbox_style="carto-darkmatter", 
        margin={"r":0,"t":0,"l":0,"b":0}, 
        mapbox_zoom=11, 
        mapbox_center={"lat": -37.8136, "lon": 144.9631}, # Default to Melbourne
        showlegend=True,
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01, bgcolor="rgba(0,0,0,0.5)")
    )
    
    # 3. GAUGE LOGIC
    gauge_fig = go.Figure(go.Indicator(
        mode="gauge+number", 
        value=otp_val, 
        number={'font':{'color':'white', 'size': 24}, 'suffix': '%'},
        gauge={
            'axis': {'range': [0, 100], 'tickcolor': "white"},
            'bar': {'color': '#10b981' if otp_val > 80 else '#fbbf24'}, 
            'bgcolor': 'rgba(255,255,255,0.05)'
        }
    ))
    gauge_fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)', font={'color': "white"}, 
        margin=dict(l=35, r=35, t=10, b=30), height=160
    )

    # 4. KPI & STATUS UPDATES
    status = f"Live Sync: {datetime.now().strftime('%H:%M:%S')}"
    active_count = str(len(df_live))
    routes_count = len(df_live) + 8

    if not df_live.empty:
        latest_ts = df_live['timestamp'].astype(int).max()
        seconds_ago = int(datetime.now().timestamp()) - latest_ts
        latency_text = f"{seconds_ago}s ago"
    else:
        latency_text = "N/A"


    #5 Line with delay
    # 2. Delay Trend Chart
    delay_fig = go.Figure(go.Scatter(x=df_delay['time'], y=df_delay['delay'], fill='tozeroy', 
                                     line=dict(color='#38bdf8', width=2), marker=dict(size=4)))
    delay_fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(color="white", size=10),
                            margin=dict(l=30, r=10, t=10, b=30), xaxis=dict(showgrid=False), yaxis=dict(gridcolor="rgba(255,255,255,0.1)"))

    return (
        map_fig, 
        delay_fig, 
        gauge_fig,
        create_route_leaderboard(routes), 
        create_stop_leaderboard(stops), 
        status,
        active_count,
        latency_text,
        routes_count
    )

if __name__ == '__main__':
    # Set debug=False in production
    app.run(host='0.0.0.0', port=8050, debug=True)