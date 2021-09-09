import aircraftlib as aclib
from prefect import task, Flow, Parameter


@task
def extract_reference_data(...):
    # fetch reference data
    print('fetching reference data...')
    return aclib.fetch_reference_data()


@task
def extract_live_data(airport, radius, ref_data):
    # Get the live aircraft vector data around the given airport (or none)
    area = None
    if airport:
        airport_data = ref_data.airports[airport]
        airport_position = aclib.Position(lat=float(airport_data['latitude']), long=float(airport_data['longitude']))  # noqa:E501
        area = aclib.bounding_box(airport_position, radius)

    print('fetching live aircraft data...')
    raw_aircraft_data = aclib.fetch_live_aircraft_data(area=area)

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
        airport = Parameter('airport', default='IAD')
        radius = Parameter('radius', default=200)

        reference_data = extract_reference_data()
        live_data = extract_live_data()

        transformed_live_data = transform(live_data, reference_data)

        load_reference_data(reference_data)
        load_live_data(live_data)

        # Run the flow with default airport='IAD' & radius=200
        flow.run()

        # Run the flow with overridden airport='DCA' and default radius=200
        flow.run(airport='DCA')

        # Run the flow with overridden airport='DCA' and radius=300
        flow.run(airport='DCA', radius=300)


if __name__ == '__main__':
    main()
