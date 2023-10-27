# Copyright (c) 2021, MD2K Center of Excellence
# - Nasir Ali <nasir.ali08@gmail.com>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from datetime import timedelta
import random
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, ModuleMetadata
from cerebralcortex.core.util.spark_helper import get_or_create_sc


def gen_location_data(CC, study_name, user_id, gps_stream_name, location_stream_name, start_time, end_time):
    """
    Create pyspark dataframe with some sample gps data (Memphis, TN, lat, long, alt coordinates)

    Args:
        user_id (str): id of a user
        stream_name (str): sample gps stream name

    Returns:
        DataStream: datastream object of gps location stream with its metadata

    """
    gps_data_columns = ["timestamp", "localtime", "user", "version", "latitude", "longitude", "altitude", "speed", "bearing", "accuracy"]
    semantic_location_columns = ["timestamp", "localtime", "user", "version", "window", "semantic_name"]
    semantic_locations = ["home", "work", "gym", "shopping-mall"]
    gps_data = []
    semantic_locations_data = []
    sql_context = get_or_create_sc("sqlContext")

    lower_left = [35.079678, -90.074136]
    upper_right = [35.194771, -89.868766]
    alt = [i for i in range(83, 100)]

    total_seconds = round((end_time - start_time).total_seconds())
    # semantic location

    # GPS coordinates
    lat = random.uniform(lower_left[0], upper_right[0])
    long = random.uniform(lower_left[1], upper_right[1])

    for dp in range(total_seconds):
        lat_val = random.gauss(lat, 0.001)
        long_val = random.gauss(long, 0.001)
        alt_val = random.choice(alt)

        speed_val = round(random.uniform(0.0,5.0),6)
        bearing_val = round(random.uniform(0.0,350),6)
        accuracy_val = round(random.uniform(10.0, 30.4),6)
        timestamp = start_time + timedelta(seconds=dp)
        localtime = start_time + timedelta(seconds=dp)
        gps_data.append((timestamp, localtime, user_id, 1, lat_val, long_val, alt_val, speed_val, bearing_val, accuracy_val))
        semantic_locations_data.append((timestamp, localtime, user_id, 1, (timestamp - timedelta(seconds=1), timestamp), random.choice(semantic_locations)))

    df = sql_context.createDataFrame(gps_data, gps_data_columns)
    print(f'gps data: length:{df.count()}')
    df.show(10)
    gps_coordinates_metadata2 = gps_coordinates_metadata(study_name=study_name, stream_name=gps_stream_name)
    ds = DataStream(data=df, metadata=gps_coordinates_metadata2)
    CC.save_stream(ds, overwrite=True)

    df2 = sql_context.createDataFrame(semantic_locations_data, semantic_location_columns)
    print(f'semantic location data: length: {df2.count()}')
    df2.show(10)
    gps_location_metadata2 = semantic_location_metadata(study_name=study_name, stream_name=location_stream_name)
    ds2 = DataStream(data=df2, metadata=gps_location_metadata2)
    CC.save_stream(ds2, overwrite=True)


def gps_coordinates_metadata(study_name, stream_name):
    stream_metadata = Metadata()
    stream_metadata.set_study_name(study_name).set_name(stream_name).set_description("GPS sample data stream.") \
        .add_dataDescriptor(
        DataDescriptor().set_name("timestamp").set_type("datetime").set_attribute("description", "UTC timestamp of data point collection.")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("localtime").set_type("datetime").set_attribute("description", "local timestamp of data point collection.")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("user").set_type("string").set_attribute("description", "user id")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("version").set_type("int").set_attribute("description", "version of the data")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("latitude").set_type("float").set_attribute("description", "gps latitude")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("longitude").set_type("float").set_attribute("description", "gps longitude")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("altitude").set_type("float").set_attribute("description", "gps altitude")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("speed").set_type("float").set_attribute("description", "speed info")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("bearing").set_type("float").set_attribute("description", "bearing info")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("accuracy").set_type("float").set_attribute("description", "accuracy of gps location")) \
        .add_module(
        ModuleMetadata().set_name("examples.util.data_helper.gen_location_data").set_attribute("attribute_key", "attribute_value").set_author(
            "Nasir Ali", "software@md2k.org"))
    stream_metadata.is_valid()

    return stream_metadata

def semantic_location_metadata(study_name, stream_name):
    stream_metadata = Metadata()
    stream_metadata.set_study_name(study_name).set_name(stream_name).set_description("GPS sample data stream.") \
        .add_dataDescriptor(
        DataDescriptor().set_name("timestamp").set_type("datetime").set_attribute("description", "UTC timestamp of data point collection.")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("localtime").set_type("datetime").set_attribute("description", "local timestamp of data point collection.")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("user").set_type("string").set_attribute("description", "user id")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("version").set_type("int").set_attribute("description", "version of the data")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("window").set_type("struct").set_attribute("description", "values are computed over a window duration (window-start-time: timestamp, window-end-time: timestamp + window_duration columns). window duration is in seconds.")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("semantic_name").set_type("string").set_attribute("description", "semantic name of the gps co-ordinate")) \
        .add_module(
        ModuleMetadata().set_name("examples.util.data_helper.gen_location_data").set_attribute("attribute_key", "attribute_value").set_author(
            "Nasir Ali", "software@md2k.org"))
    stream_metadata.is_valid()
    return stream_metadata


# def gen_location_datastream(CC, study_name, user_id, stream_name, start_time=None, end_time=None):
#     """
#     Create pyspark dataframe with some sample gps data (Memphis, TN, lat, long, alt coordinates)
#
#     Args:
#         user_id (str): id of a user
#         stream_name (str): sample gps stream name
#
#     Returns:
#         DataStream: datastream object of gps location stream with its metadata
#
#     """
#     column_name = ["timestamp", "localtime", "user" ,"version" ,"latitude" ,"longitude" ,"altitude" ,"speed" ,"bearing" ,"accuracy"]
#     sample_data = []
#     timestamp = start_time
#     sqlContext = get_or_create_sc("sqlContext")
#
#     lower_left = [35.079678, -90.074136]
#     upper_right = [35.194771, -89.868766]
#     alt = [i for i in range(83,100)]
#
#     for location in range(5):
#         lat  = random.uniform(lower_left[0],upper_right[0])
#         long = random.uniform(lower_left[1],upper_right[1])
#         for dp in range(150):
#             lat_val = random.gauss(lat,0.001)
#             long_val = random.gauss(long,0.001)
#             alt_val = random.choice(alt)
#
#             speed_val = round(random.uniform(0.0,5.0),6)
#             bearing_val = round(random.uniform(0.0,350),6)
#             accuracy_val = round(random.uniform(10.0, 30.4),6)
#             random_time_delta = randrange(3, 10)
#             timestamp = timestamp + timedelta(minutes=random_time_delta)
#             localtime = timestamp + timedelta(hours=5)
#             sample_data.append((timestamp, localtime, user_id, 1, lat_val, long_val, alt_val, speed_val, bearing_val, accuracy_val))
#
#     df = sqlContext.createDataFrame(sample_data, column_name)
#
#     stream_metadata = Metadata()
#     stream_metadata.set_study_name(study_name).set_name(stream_name).set_description("GPS sample data stream.") \
#         .add_dataDescriptor(
#         DataDescriptor().set_name("timestamp").set_type("datetime").set_attribute("description", "UTC timestamp of data point collection.")) \
#         .add_dataDescriptor(
#         DataDescriptor().set_name("localtime").set_type("datetime").set_attribute("description", "local timestamp of data point collection.")) \
#         .add_dataDescriptor(
#         DataDescriptor().set_name("user").set_type("string").set_attribute("description", "user id")) \
#         .add_dataDescriptor(
#         DataDescriptor().set_name("version").set_type("int").set_attribute("description", "version of the data")) \
#         .add_dataDescriptor(
#         DataDescriptor().set_name("latitude").set_type("float").set_attribute("description", "gps latitude")) \
#         .add_dataDescriptor(
#         DataDescriptor().set_name("longitude").set_type("float").set_attribute("description", "gps longitude")) \
#         .add_dataDescriptor(
#         DataDescriptor().set_name("altitude").set_type("float").set_attribute("description", "gps altitude")) \
#         .add_dataDescriptor(
#         DataDescriptor().set_name("speed").set_type("float").set_attribute("description", "speed info")) \
#         .add_dataDescriptor(
#         DataDescriptor().set_name("bearing").set_type("float").set_attribute("description", "bearing info")) \
#         .add_dataDescriptor(
#         DataDescriptor().set_name("accuracy").set_type("float").set_attribute("description", "accuracy of gps location")) \
#         .add_module(
#         ModuleMetadata().set_name("examples.util.data_helper.gen_location_data").set_attribute("attribute_key", "attribute_value").set_author(
#             "Nasir Ali", "software@md2k.org"))
#     stream_metadata.is_valid()
#
#     ds = DataStream(data=df, metadata=stream_metadata)
#     CC.save_stream(ds)
#
#
# def gen_semantic_location_datastream(CC, study_name, user_id, stream_name, start_time, end_time):
#     """
#     Create pyspark dataframe with some sample gps data (Memphis, TN, lat, long, alt coordinates)
#
#     Args:
#         user_id (str): id of a user
#         stream_name (str): sample gps stream name
#
#     Returns:
#         DataStream: datastream object of gps location stream with its metadata
#
#     """
#     column_name = ["timestamp", "localtime", "user" ,"version" ,"window" ,"semantic_name"]
#     sample_data = []
#     timestamp = start_time
#     sqlContext = get_or_create_sc("sqlContext")
#
#     semantic_locations = ["home", "work", "gym", "shopping-mall"]
#
#     for dp in range(12):
#         slocation = random.choice(semantic_locations)
#         window = (timestamp, timestamp + timedelta(minutes=10))
#         timestamp = timestamp + timedelta(minutes=10)
#         localtime = timestamp + timedelta(minutes=10)
#
#         sample_data.append((timestamp, localtime, user_id, 1, window, slocation))
#
#     df = sqlContext.createDataFrame(sample_data, column_name)
#
#     stream_metadata = Metadata()
#     stream_metadata.set_study_name(study_name).set_name(stream_name).set_description("GPS sample data stream.") \
#         .add_dataDescriptor(
#         DataDescriptor().set_name("timestamp").set_type("datetime").set_attribute("description", "UTC timestamp of data point collection.")) \
#         .add_dataDescriptor(
#         DataDescriptor().set_name("localtime").set_type("datetime").set_attribute("description", "local timestamp of data point collection.")) \
#         .add_dataDescriptor(
#         DataDescriptor().set_name("user").set_type("string").set_attribute("description", "user id")) \
#         .add_dataDescriptor(
#         DataDescriptor().set_name("version").set_type("int").set_attribute("description", "version of the data")) \
#         .add_dataDescriptor(
#         DataDescriptor().set_name("window").set_type("struct").set_attribute("description", "values are computed over a window duration (window-start-time: timestamp, window-end-time: timestamp + window_duration columns). window duration is in seconds.")) \
#         .add_dataDescriptor(
#         DataDescriptor().set_name("semantic_name").set_type("string").set_attribute("description", "semantic name of the gps co-ordinate")) \
#         .add_module(
#         ModuleMetadata().set_name("examples.util.data_helper.gen_location_data").set_attribute("attribute_key", "attribute_value").set_author(
#             "Nasir Ali", "software@md2k.org"))
#     stream_metadata.is_valid()
#     ds = DataStream(data=df, metadata=stream_metadata)
#     CC.save_stream(ds)
