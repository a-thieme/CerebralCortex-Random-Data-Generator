from CerebralCortexRandomDataGenerator.ccrdg.battery_data import gen_battery_data
from CerebralCortexRandomDataGenerator.ccrdg.accel_gyro_data import gen_accel_gyro_data
from CerebralCortexRandomDataGenerator.ccrdg.location_data import gen_location_data

from cerebralcortex.kernel import Kernel

import datetime

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

def get_cc(start_time:datetime.datetime, end_time:datetime.datetime, study_name='mguard', user_id='dd40c'):
    # convert the no of minutes to hours for gps and semantic location data

    cc = Kernel(cc_configs="default", study_name=study_name, new_study=True)

    battery_stream_name = "ndn--org--md2k--{}--{}--phone--battery".format(study_name, user_id)
    gps_stream_name = "ndn--org--md2k--{}--{}--phone--gps".format(study_name, user_id)
    semantic_location_stream_name = "ndn--org--md2k--{}--{}--data_analysis--gps_episodes_and_semantic_location".format(
        study_name, user_id)
    accel_stream_name = "ndn--org--md2k--{}--{}--phone--accelerometer".format(study_name, user_id)
    gyro_stream_name = "ndn--org--md2k--{}--{}--phone--gyroscope".format(study_name, user_id)

    gen_battery_data(cc, study_name=study_name, user_id=user_id, stream_name=battery_stream_name, start_time=start_time,
                     end_time=end_time)
    gen_location_data(cc, study_name=study_name, user_id=user_id, gps_stream_name=gps_stream_name,
                      location_stream_name=semantic_location_stream_name, start_time=start_time, end_time=end_time)
    gen_accel_gyro_data(cc, study_name=study_name, user_id=user_id, stream_name=accel_stream_name,
                        start_time=start_time, end_time=end_time, frequency=1)
    gen_accel_gyro_data(cc, study_name=study_name, user_id=user_id, stream_name=gyro_stream_name, start_time=start_time,
                        end_time=end_time, frequency=1)

    return cc, {
        'semantic_location': semantic_location_stream_name,
        'battery': battery_stream_name,
        'accel': accel_stream_name,
        'gyro': gyro_stream_name,
        'gps': gps_stream_name
    }