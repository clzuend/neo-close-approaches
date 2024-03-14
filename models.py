"""Represent models for near-Earth objects and their close approaches.

The `NearEarthObject` class represents a near-Earth object. Each has a unique
primary designation, an optional unique name, an optional diameter, and a flag
for whether the object is potentially hazardous.

The `CloseApproach` class represents a close approach to Earth by an NEO. Each
has an approach datetime, a nominal approach distance, and a relative approach
velocity.

A `NearEarthObject` maintains a collection of its close approaches, and a
`CloseApproach` maintains a reference to its NEO.
"""
from helpers import cd_to_datetime, datetime_to_str


class NearEarthObject:
    """A near-Earth object (NEO).

    An NEO encapsulates semantic and physical parameters about the object,
    such as its primary designation (required, unique), IAU name (optional),
    diameter in kilometers (optional - sometimes unknown), and whether it's
    marked as potentially hazardous to Earth.

    A `NearEarthObject` also maintains a collection of its close approaches -
    initialized to an empty collection, but eventually populated in the
    `NEODatabase` constructor.
    """

    def __init__(self, pdes: str, name: str, diameter: float, pha: str):
        """Create a new `NearEarthObject`.

        :param pdes: The NEO’s primary designation
        :param name: The NEO’s IAU name (possibly missing)
        :param diameter: The NEO’s diameter in kilometers, or NaN
        :param pha: Flag whether the NEO is potentially hazardous
        """
        self.designation = str(pdes)
        self.name = str(name) if str(name) != 'nan' else None
        self.diameter = float(diameter) or float('nan')
        self.hazardous = True if pha == 'Y' else False

        # Create an empty initial collection of linked approaches.
        self.approaches = []

    @property
    def fullname(self):
        """Return a representation of the full name of this NEO."""
        if self.name is not None:
            return f'{self.designation} ({self.name})'
        else:
            return f'{self.designation}'

    def __str__(self):
        """Return a human-readable summary of this NEO."""
        if str(self.diameter) != 'nan':
            diameter_str = f'a diameter of {self.diameter:.3f} km'
        else:
            diameter_str = 'an unkown diameter'

        if self.hazardous is True:
            hazard_str = 'potentially hazardous'
        else:
            hazard_str = 'not potentially hazardous'

        return f'NEO {self.fullname!r} has {diameter_str} and is {hazard_str}.'

    def __repr__(self):
        """Return a computer-readable string representation of this NEO."""
        return f"NearEarthObject(designation={self.designation!r}, " \
               f"name={self.name!r}, diameter={self.diameter:.3f}, " \
               f"hazardous={self.hazardous!r})"

    def serialize(self):
        """Return a dict with a serialized representation of this NEO."""
        return {'designation': self.designation,
                'name': self.name,
                'diameter_km': self.diameter,
                'potentially_hazardous': self.hazardous
                }


class CloseApproach:
    """A close approach to Earth by an NEO.

    A `CloseApproach` encapsulates information about the NEO's close approach
    to Earth, such as the date and time (in UTC) of closest approach, the
    nominal approach distance in astronomical units, and the relative approach
    velocity in kilometers per second.

    A `CloseApproach` also maintains a reference to its `NearEarthObject` -
    initially, this information (the NEO's primary designation) is saved in a
    private attribute, but the referenced NEO is eventually replaced in the
    `NEODatabase` constructor.
    """

    def __init__(self, pdes: str, time: str, distance: float, velocity: float):
        """Create a new `CloseApproach`.

        :param pdes: The NEO’s primary designation
        :param name: The time of the approach
        :param diameter: The closet distance of the approach in astro. units
        :param velocity: The relative velocity of the approach at its closest
        """
        self._designation = str(pdes)
        self.time = cd_to_datetime(time)
        self.distance = float(distance)
        self.velocity = float(velocity)

        # Create an attribute for the referenced NEO, originally None.
        self.neo = None

    def set_neo(self, pdes_to_neo):
        """Set NEO attribute."""
        if self._designation in pdes_to_neo:
            self.neo = pdes_to_neo[self._designation]
            self.neo.approaches.append(self)
        return self

    @property
    def time_str(self):
        """Return a formatted representation of this `CloseApproach`'s time.

        The value in `self.time` should be a Python `datetime` object.
        While a `datetime` object has a string representation, the default
        representation includes seconds - significant figures that don't exist
        in our input data set.

        The `datetime_to_str` method converts a `datetime` object to a
        formatted string that can be used in human-readable representations and
        in serialization to CSV and JSON files.
        """
        return datetime_to_str(self.time)

    def __str__(self):
        """Return a human-readable string representation of this object."""
        return f"On {self.time_str!r}, {self.neo.fullname!r} " \
               f"approaches Earth at a distance of {self.distance:.2f} au " \
               f"and a velocity of {self.velocity:.2f} km/s."

    def __repr__(self):
        """Return a computer-readable string representation of this object."""
        return f"CloseApproach(time={self.time_str!r}, " \
               f"distance={self.distance:.2f}, " \
               f"velocity={self.velocity:.2f}, " \
               f"neo={self.neo!r})"

    def serialize(self):
        """Return a dict with a serialized representation of this object."""
        return {'datetime_utc': self.time_str,
                'distance_au': self.distance,
                'velocity_km_s': self.velocity
                }
