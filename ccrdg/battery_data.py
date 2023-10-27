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

from datetime import datetime, timedelta
from cerebralcortex.core.datatypes import DataStream
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata, DataDescriptor, ModuleMetadata
from cerebralcortex.core.util.spark_helper import get_or_create_sc

def gen_battery_data(CC, study_name, user_id, stream_name, version=1, start_time=None, end_time=None):
    """
    Create pyspark dataframe with some sample phone battery data
    Returns:
        DataFrame: pyspark dataframe object with columns: ["timestamp", "battery_level", "version", "user"]

    """
    column_name = ["timestamp", "localtime", "user" ,"version", "level", "voltage", "temperature"]
    sample_data = []
    timestamp = start_time
    sample = 100
    voltage = 3700
    temperature = 70
    sqlContext = get_or_create_sc("sqlContext")
    total_data = round((end_time-start_time).total_seconds())
    for row in range(total_data):
        sample = sample - 0.01
        localtime = timestamp - timedelta(hours=5)
        sample_data.append((timestamp, localtime, user_id, version, sample, voltage, temperature))
        timestamp = timestamp + timedelta(seconds=1)

    df = sqlContext.createDataFrame(sample_data, column_name)
    print(f'battery_data: length: {df.count()}')
    df.show(10)
    stream_metadata = Metadata()
    stream_metadata.set_study_name(study_name).set_name(stream_name).set_description("battery sample data stream.") \
        .add_dataDescriptor(
        DataDescriptor().set_name("timestamp").set_type("datetime").set_attribute("description", "UTC timestamp of data point collection.")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("localtime").set_type("datetime").set_attribute("description", "local timestamp of data point collection.")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("user").set_type("string").set_attribute("description", "user id")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("version").set_type("int").set_attribute("description", "version of the data")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("level").set_type("float").set_attribute("description", "current battery charge")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("voltage").set_type("float").set_attribute("description", "current battery voltage level")) \
        .add_dataDescriptor(
        DataDescriptor().set_name("temperature").set_type("float").set_attribute("description", "current battery temperature")) \
        .add_module(
        ModuleMetadata().set_name("battery").set_version("1.2.4").set_attribute("attribute_key", "attribute_value").set_author(
            "Nasir Ali", "software@md2k.com"))
    stream_metadata.is_valid()

    ds = DataStream(df, stream_metadata)
    CC.save_stream(ds, overwrite=True)


