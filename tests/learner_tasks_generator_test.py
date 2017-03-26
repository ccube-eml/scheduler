import unittest

from scheduler.tasks_generators import learner_tasks_generator

JOB_NAME = 'gpfunction'
TASKS_NUMBER = 10

DATASET_NAME = 'higgs'
TRAINING_RATE = 0.5
FUSION_RATE = 0.3
TRAINING_SAMPLE_RATE = 0.1
CLASS_ATTRIBUTE = 'label'
CLASS_ATTRIBUTE_TYPE = 'integer'
TRUE_CLASS_ATTRIBUTE = 1
INCLUDE_ATTRIBUTES = []
EXCLUDE_ATTRIBUTES = []
ATTRIBUTES_RATE = 0.5
RANDOM_SEED = 0
INCLUDE_HEADER = False
DURATION = 60

LEARNER_PARAMETERS = [
    {
        'name': 'xover_op',
        'type': 'text',
        'values': [
            'SPUCrossover',
            'KozaCrossover',
        ],
    },
    {
        'name': 'pop_size',
        'type': 'integer',
        'range': {
            'start': 1000,
            'stop': 2000,
            'step': 100,
        }
    },
    {
        'name': 'mutation_rate',
        'type': 'real',
        'range': {
            'start': 0.1,
            'stop': 1.0,
            'step': 0.001,
        }
    },
]


class LearnerTasksGeneratorTest(unittest.TestCase):
    def setUp(self):
        self.__tasks_generator = learner_tasks_generator.LearnerTasksGenerator(
            job_name=JOB_NAME,
            tasks_number=TASKS_NUMBER,
            dataset_name=DATASET_NAME,
            training_rate=TRAINING_RATE,
            fusion_rate=FUSION_RATE,
            sample_rate=TRAINING_SAMPLE_RATE,
            class_attribute=CLASS_ATTRIBUTE,
            class_attribute_type=CLASS_ATTRIBUTE_TYPE,
            true_class_value=TRUE_CLASS_ATTRIBUTE,
            include_attributes=INCLUDE_ATTRIBUTES,
            exclude_attributes=EXCLUDE_ATTRIBUTES,
            attributes_rate=ATTRIBUTES_RATE,
            random_seed=RANDOM_SEED,
            include_header=INCLUDE_HEADER,
            duration=DURATION,
            learner_parameters=LEARNER_PARAMETERS,
        )

    def tearDown(self):
        pass

    def test_tasks_generator(self):
        for task in self.__tasks_generator:
            print(task)
