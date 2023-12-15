import random
import pandas as pd
from bokeh.driving import count
from bokeh.models import ColumnDataSource
from bokeh.plotting import curdoc, figure, show
from bokeh.models import DatetimeTickFormatter
from bokeh.models.widgets import Div
from bokeh.layouts import column, row
import time
from datetime import datetime
from confluent_kafka import Consumer

# Set interval waktu untuk pembaruan aplikasi Bokeh (dalam detik)
UPDATE_INTERVAL = 1
# Jumlah titik data yang ditampilkan
ROLLOVER = 10

# Membuat ColumnDataSource untuk menyimpan data plot Bokeh
source = ColumnDataSource({"x": [], "y": []})

# Konfigurasi Consumer Kafka
consumer = Consumer({
    'bootstrap.servers': 'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'HZ353Z5ZYNCOECD5',
    'sasl.password': '60NbFFdoYx9FbBQJ/yzctTzU1SxNz0bwgBOcThzUt1YwXyHdtVYpQY+UY1d2X8BR',
    'group.id': 'python_example_group_1',
    'auto.offset.reset': 'earliest'
})

# Berlangganan ke topik Kafka
consumer.subscribe(['edodf-data-clean'])

# Membuat widget Div untuk menampilkan informasi
div = Div(text='', width=120, height=35)

# Fungsi callback Bokeh yang memperbarui plot
@count()
def update(x):
    msg_value = None
    # Poll pesan Kafka
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            print("Menunggu...")
            return
        elif msg.error():
            print("ERROR: %s" % msg.error())
            return
        else:
            print("Topik {}, Pesan: {}".format(msg.topic(), msg.value().decode('utf-8')))
            msg_value = msg
            break

    # Mem-parsing nilai pesan Kafka
    values = eval(msg_value.value().decode("utf-8"))
    x = pd.to_datetime(values["measurement_timestamp"])
    
    print(x)

    # Memperbarui widget Div dengan informasi timestamp
    div.text = "TimeStamp: " + str(x)

    # Ekstrak nilai suhu air
    y = values['water_temperature']
    print(y)
    
    # Mengirimkan data point baru ke sumber plot Bokeh
    source.stream({"x": [x], "y": [y]}, ROLLOVER)

# Membuat plot Bokeh
p = figure(title="Data Sensor Suhu Air", x_axis_type="datetime", width=1000)
p.line("x", "y", source=source)

# Mengkonfigurasi tampilan plot
p.xaxis.formatter = DatetimeTickFormatter(hourmin=['%H:%M'])
p.xaxis.axis_label = 'Waktu'
p.yaxis.axis_label = 'Nilai'
p.title.align = "right"
p.title.text_color = "orange"
p.title.text_font_size = "25px"

# Menyiapkan dokumen Bokeh
doc = curdoc()

# Menambahkan widget Div dan plot ke dalam dokumen
doc.add_root(row(children=[div, p]))

# Menambahkan callback periodik untuk memperbarui plot secara teratur
doc.add_periodic_callback(update, UPDATE_INTERVAL)
