import random

from scheduler.tasks_generators.tasks_generator import TasksGenerator


INTEGER_DEFAULT_STEP = 1
FLOAT_DEFAULT_STEP = 0.001


class LearnerTasksGenerator(TasksGenerator):
    """
    Generates the learner tasks.
    """

    def __init__(
            self,
            job_name,
            tasks_number,
            dataset_name,
            training_rate,
            fusion_rate,
            sample_rate,
            class_attribute,
            class_attribute_type,
            true_class_value,
            include_attributes,
            exclude_attributes,
            attributes_rate,
            random_seed,
            include_header,
            duration,
            learn_parameters,
    ):
        """
        Initializes the generator.

        :param job_name: the name of the job
        :type job_name: str

        :param tasks_number: the total number of tasks.
        :type tasks_number: int

        :param dataset_name: the name of the dataset
        :type dataset_name: str

        :param training_rate: the percentage of the dataset to consider as training split
        :type training_rate: float

        :param fusion_rate: the percentage of the dataset to consider as fusion split
        :type fusion_rate: float

        :param sample_rate: the percentage of instances, within the training split, to include
        :type sample_rate: float

        :param class_attribute: the name of the class attribute
        :type class_attribute: str

        :param class_attribute_type: the type of the class attribute, 'integer' | 'real' | 'text'
        :type class_attribute_type: str

        :param true_class_value: the class value considered as true
        :type true_class_value: object

        :param include_attributes: the list of the attributes to include, None otherwise
        :type include_attributes: list[str]

        :param exclude_attributes: the list of the attributes to exclude, None otherwise
        :type exclude_attributes: list[str]

        :param attributes_rate: the percentage of attributes to include
        :type attributes_rate: float

        :param random_seed: the random seed
        :type random_seed: int

        :param include_header: if True, it includes the header in the output CSV
        :type include_header: bool

        :param duration: the duration in seconds
        :type duration: int

        :param learn_parameters: the list of parameters for the learner,
         in the form {'name': str, 'type': 'integer' | 'real' | 'text', 'values': list[object],
         'range': {'start': int, 'stop': int, 'step': int}}
        :type learn_parameters: list[object]
        """
        super().__init__()

        self.__job_name = job_name
        self.__tasks_number = tasks_number
        self.__dataset_name = dataset_name
        self.__training_rate = training_rate
        self.__fusion_rate = fusion_rate
        self.__sample_rate = sample_rate
        self.__class_attribute = class_attribute
        self.__class_attribute_type = class_attribute_type
        self.__true_class_value = true_class_value
        self.__include_attributes = include_attributes
        self.__exclude_attributes = exclude_attributes
        self.__attributes_rate = attributes_rate
        self.__random_seed = random_seed
        self.__include_header = include_header
        self.__duration = duration
        self.__learn_parameters = learn_parameters

        self.__i = 0
        self.__random_generator = random.Random(random_seed)

    def __iter__(self):
        return self

    def __next__(self):
        if self.__i < self.__tasks_number:
            task = {
                'job_name': self.__job_name,
                'task_number': self.__i,
                'dataset_name': self.__dataset_name,
                'training_rate': self.__training_rate,
                'fusion_rate': self.__fusion_rate,
                'sample_rate': self.__sample_rate,
                'class_attribute': self.__class_attribute,
                'class_attribute_type': self.__class_attribute_type,
                'true_class_value': self.__true_class_value,
                'include_attributes': self.__include_attributes,
                'exclude_attributes': self.__exclude_attributes,
                'attributes_rate': self.__attributes_rate,
                'random_seed': self.__random_seed,
                'include_header': self.__include_header,
                'duration': self.__duration,
                'learn_parameters': self.__next_learn_parameters(),
            }
            self.__i += 1
            return task
        raise StopIteration

    def __next_learn_parameters(self):
        """
        Returns the next learn parameters, according to the random generator.

        :return: a dictionary with the parameters in the form {name: value}
        :rtype: dict[str, object]
        """
        parameters = {}
        for parameter in self.__learn_parameters:
            name = parameter.get('name')
            value_type = parameter.get('type')
            values = None

            if 'values' in parameter:
                values = [self._convert_string(x, value_type) for x in parameter.get('values')]
            elif 'range' in parameter:
                range_start = parameter.get('range').get('start')
                range_stop = parameter.get('range').get('stop')
                range_step = parameter.get('range').get('step')

                values = self._get_range(range_start, range_stop, range_step, value_type)

            value = self.__random_generator.choice(values)

            parameters[name] = value

        return parameters
