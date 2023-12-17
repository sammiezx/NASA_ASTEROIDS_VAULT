

#INTEGRITY TEST / CONSTRAINT
def check_constraints(value):
    if len(value) != 1:
        raise ValueError(f"[close_approach_data] can't have multiple or no value")
    return


### This method checks and makes sure for constrainst and handles units which conform to SI Units
def parse_response(data):
    data['links'] = data['links'].map(lambda x: dict(x)['self'])
    #ADHERING TO MKS UNITS
    data['estimated_diameter_max'] = data['estimated_diameter'].map(lambda x: dict(x)['meters']['estimated_diameter_max'])
    data['estimated_diameter_min'] = data['estimated_diameter'].map(lambda x: dict(x)['meters']['estimated_diameter_min'])
    data = data.drop('estimated_diameter', axis=1)

    data['close_approach_data'].apply(check_constraints)

    data['close_approach_date'] = data['close_approach_data'].map(lambda x: list(x)[0]['close_approach_date'])
    data['close_approach_date_full'] = data['close_approach_data'].map(lambda x: list(x)[0]['close_approach_date_full'])
    data['epoch_date_close_approach'] = data['close_approach_data'].map(lambda x: list(x)[0]['epoch_date_close_approach'])

    # changing kilometers per second to meters per second in the command below
    data['relative_velocity'] = data['close_approach_data'].map(lambda x: float(list(x)[0]['relative_velocity']['kilometers_per_second'])*1000) #ADHERING TO MKS UNITS

    data['miss_distance_astronomical'] = data['close_approach_data'].map(lambda x: list(x)[0]['miss_distance']['astronomical'])
    data['miss_distance_lunar'] = data['close_approach_data'].map(lambda x: list(x)[0]['miss_distance']['lunar'])

    # changing kilometers to meters in the command below
    data['miss_distance_meters'] = data['close_approach_data'].map(lambda x: float(list(x)[0]['miss_distance']['kilometers'])*1000) #ADHERING TO MKS UNITS

    data['orbiting_body'] = data['close_approach_data'].map(lambda x: list(x)[0]['orbiting_body'])

    data = data.drop('close_approach_data', axis=1)

    return data