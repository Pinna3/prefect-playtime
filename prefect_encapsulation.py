import aircraftlib as aclib
from prefect import task, Flow


@task
def extract_reference_data(...):
    # fetch reference data
    print('fetching reference data...')
    return aclib.fetch_reference_data()


@task
def extract_live_data(...):
    # Get the live aircraft vector data around Dulles airport
    dulles_airport_position = aclib.Position(lat=38.951, long=77.448)
    area_surrounding_dulles = aclib.bounding_box(
        dulles_airport_position, radius_km=200)

    print('fetching live aircraft data...')
    raw_aircraft_data = aclib.fetch_live_aircraft_data(
        area=area_surrounding_dulles)

    return raw_aircraft_data


@task
def transform(raw_aircraft_data, reference_data):
    # clean the live data
    print('cleaning & transforming aircraft data...')

    live_aircraft_data = []
    for raw_vector in raw_aircraft_data:
        vector = aclib.clean_vector(raw_vector)
        if vector:
            aclib.add_airline_info(vector, reference_data.airlines)
            live_aircraft_data.append(vector)

    return live_aircraft_data


@task
def load_reference_data(reference_data):
    # save the reference data to the database
    print('saving reference data...')
    db = aclib.Database()
    db.update_reference_data(reference_data)


@task
def load_live_data(live_aircraft_data):
    # save the transformed live data to the database
    print('saving live aircraft data...')
    db = aclib.Database()
    db.add_live_aircraft_data(live_aircraft_data)


def main():
    with Flow('Aircraft-ETL') as flow:
        reference_data = extract_reference_data()
        live_data = extract_live_data()

        transformed_live_data = transform(live_data, reference_data)

        load_reference_data(reference_data)
        load_live_data(transformed_live_data)

    flow.run()


if __name__ == '__main__':
    main()
