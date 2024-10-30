import constants
from dash import Dash, dcc, html, Input, Output, State, no_update, callback, ctx
import dash_design_kit as ddk
import pandas as pd
import plotly.express as px
import json
import db
from dateutil.parser import isoparse

from tsdownsample import LTTBDownsampler

max_points = 25_000
max_buckets = 100_000

app = Dash(__name__)
server = app.server  # expose server variable for Procfile

with open('config.json', 'r') as fp:
    config = json.load(fp)

variable_options = []

for var in config:
    variable_options.append({'value': var, 'label': config[var]['long_name']})

controls = [
    ddk.ControlItem(
        dcc.Dropdown(id='drone', options=[{'label': '1030', 'value': '1030.0'}, {'label': '1033', 'value': '1033.0'}, {'label':'1079', 'value': '1079.0'}], multi=False),
        label='Drone:',
    ),
    ddk.ControlItem(
        dcc.Dropdown(id='variable', options=variable_options, multi=False), label='Variable:'
    ),
    ddk.ControlItem(
        ddk.ControlCard(orientation='horizontal', children=[
            ddk.ControlItem(label='Start Date/Time:', children=[dcc.Input(id="start-date", type="text", value=constants.start_time)]),
            ddk.ControlItem(label='End Date/Time:', children=[dcc.Input(id="end-date", type="text", value=constants.end_time)])
        ])
    ),
    ddk.ControlItem(
        ddk.ControlCard(orientation='horizontal', children=[
            ddk.ControlItem(
                html.Button(id='resample', children='Resample')
            ),
            ddk.ControlItem(
                html.Button(id='reset', children='Reset')
            )
        ])
    ),
]

app.layout = ddk.App([

    ddk.Header([
        ddk.Logo(src=app.get_asset_url('NOAA_logo_mobile.svg')),
        ddk.Title('1Hz TPOS Saildrone Data'),
    ]),

    ddk.Row([
        ddk.ControlCard(controls, width=30),
        ddk.Card(width=70, children=[
            ddk.Graph(id='timeseries'),
            ddk.Card(width=1, children=[
                dcc.Loading(html.H6(id='ratio-tag'))
            ])
        ]),
    ]),
])


@app.callback(
    [
        Output('start-date', 'value'),
        Output('end-date', 'value')
    ],
    [
        Input("timeseries", "relayoutData"),
        Input('reset', 'n_clicks')
    ],
)
def set_dates(layout_data, reset_click):
    tig = ctx.triggered_id
    if tig == 'reset':
        return[constants.start_time, constants.end_time]
    if layout_data is not None and 'xaxis.range[0]' in layout_data and 'xaxis.range[1]' in layout_data:
        start_time = layout_data['xaxis.range[0]']
        end_time = layout_data['xaxis.range[1]']
    else:
        return [no_update, no_update]
    return [start_time, end_time]


@app.callback(
    [
        Output('timeseries', 'figure'),
        Output('ratio-tag', 'children')
    ],
    [
        Input('drone', 'value'),
        Input('variable', 'value'),
        Input('resample', 'n_clicks'),
        Input('reset', 'n_clicks')
    ],
    [
        State('start-date', 'value'),
        State('end-date', 'value'),
    ]
)
def make_timeseries(drone, variable, resample_click, reset_click, in_start_date, in_end_date, ):
    
    xstart = constants.xstart
    xend = constants.xend
    query_start_time = constants.start_time
    query_end_time = constants.end_time
    total_sec = xend - xstart
    tig = ctx.triggered_id

    if tig == 'resample':
        query_start_time = in_start_date
        query_end_time = in_end_date
        start_obj = isoparse(query_start_time)
        end_obj = isoparse(query_end_time)
        xstart = start_obj.timestamp()
        xend = end_obj.timestamp()

    ratio = (xend-xstart)/total_sec
    buckets = int(max_buckets*ratio)

    total_rows = xend-xstart
    
    if drone is None or len(drone) == 0 or variable is None or len(variable) == 0:
        return no_update
    else:
        if total_rows > max_points:
            s_ratio = max_points/(xend-xstart)
            ratio_text = 'Sampling ratio was ' + str(s_ratio)
            df = db.get_minmax_timeseries(drone, variable, buckets, query_start_time, query_end_time)
            index = LTTBDownsampler().downsample(df['time_seconds'], df['value'], n_out=max_points)
            df = df.iloc[index]
        else:
            df = db.get_timeseries(drone, variable, query_start_time, query_end_time)
            ratio_text = 'Sampling ratio was 1.0 with ' + str(df.shape[0]) + ' data points.'
        df['time'] = pd.to_datetime(df['time_seconds'], unit='s')
        df.rename(columns={'value': variable}, inplace=True)
        figure = px.line(df, x='time', y=variable, title=config[variable]['long_name'] + ' (' + config[variable]['units'] + ')')
        return [figure, ratio_text]


if __name__ == '__main__':
    app.run(debug=True)
