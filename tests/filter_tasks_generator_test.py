import unittest

from scheduler.tasks_generators import filter_tasks_generator

JOB_NAME = 'gpfunction'

DATASET_NAME = 'higgs'
LEARNER_OUTPUTS_NUMBER = 10
TRAINING_RATE = 0.5
FUSION_RATE = 0.3
CLASS_ATTRIBUTE = 'label'
CLASS_ATTRIBUTE_TYPE = 'integer'
TRUE_CLASS_ATTRIBUTE = 1
INCLUDE_ATTRIBUTES = []
EXCLUDE_ATTRIBUTES = []
ATTRIBUTES_RATE = 0.5
RANDOM_SEED = 0
INCLUDE_HEADER = False
THRESHOLD = 0.5

EXECUTOR_PARAMETERS = [
    {
        'name': 'xover_op',
        'type': 'text',
        'value': 'SPUCrossover',
    },
]


class FilterTasksGeneratorTest(unittest.TestCase):
    def setUp(self):
        self.__tasks_generator = filter_tasks_generator.FilterTasksGenerator(
            job_name=JOB_NAME,
            learner_outputs_number=LEARNER_OUTPUTS_NUMBER,
            dataset_name=DATASET_NAME,
            training_rate=TRAINING_RATE,
            fusion_rate=FUSION_RATE,
            class_attribute=CLASS_ATTRIBUTE,
            class_attribute_type=CLASS_ATTRIBUTE_TYPE,
            true_class_value=TRUE_CLASS_ATTRIBUTE,
            include_attributes=INCLUDE_ATTRIBUTES,
            exclude_attributes=EXCLUDE_ATTRIBUTES,
            attributes_rate=ATTRIBUTES_RATE,
            random_seed=RANDOM_SEED,
            include_header=INCLUDE_HEADER,
            threshold=THRESHOLD,
            executor_parameters=EXECUTOR_PARAMETERS,
        )

    def tearDown(self):
        pass

    def test_tasks_generator(self):
        for task in self.__tasks_generator:
            print(task)
