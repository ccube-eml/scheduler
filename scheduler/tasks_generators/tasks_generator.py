from abc import ABCMeta, abstractmethod

import numpy


INTEGER_DEFAULT_STEP = 1
FLOAT_DEFAULT_STEP = 0.001


class TasksGenerator(object):
    """
    Defines a tasks generator.
    """

    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    def _convert_string(self, value, value_type):
        """
        Converts a string to a value according to a given type.

        :param value: the value to convert
        :rtype: str

        :param value_type: the destination between 'integer', 'real' and 'string'
        :type value_type: str

        :return: the value converted
        :rtype: object
        """
        if value_type == 'integer':
            return int(value)
        elif value_type == 'real':
            return float(value)
        elif value_type == 'text':
            return value

    def _get_range(self, start, stop, step, value_type):
        """
        Gets a range according to the given value type.

        :param start: the lower limit of the range
        :type start: str

        :param stop: the upper limit of the range
        :type stop: str

        :param step: the step for the range
        :type step: str

        :param value_type: the type of the values between 'integer' and 'real'
        :type value_type: str

        :return: the range of values
        :rtype: list[object]
        """
        if value_type == 'integer':
            start = int(start)
            stop = int(stop)
            if step:
                step = int(step)
            return range(start, stop, step or INTEGER_DEFAULT_STEP)
        elif value_type == 'real':
            start = float(start)
            stop = float(stop)
            if step:
                step = float(step)
            return [float(str(x)) for x in numpy.arange(start, stop, step or FLOAT_DEFAULT_STEP)]
