import unittest
import os

from scheduler import __main__
from scheduler.amqp_manager import AMQPManager


JOB_NAME = 'gpfunction'
LEARNERS_NUMBER = 10

DATASET_NAME = 'higgs'
TRAINING_RATE = 0.5
FUSION_RATE = 0.3
TRAINING_SAMPLE_RATE = 0.1
CLASS_ATTRIBUTE = 'label'
CLASS_ATTRIBUTE_TYPE = 'text'
TRUE_CLASS_VALUE = 1
INCLUDE_ATTRIBUTES = []
EXCLUDE_ATTRIBUTES = []
ATTRIBUTES_RATE = 0.5
RANDOM_SEED = 0
INCLUDE_HEADER = False
DURATION = 60
THRESHOLD = 0.5

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
        'name': 'cpp',
        'type': 'integer',
        'values': [
            1,
            2,
            4,
        ]
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

EXECUTOR_PARAMETERS = [
    {
        'name': 'test_1',
        'type': 'real',
        'value': 0.1,
    }
]

AMQP_HOSTNAME = 'localhost'


class MainTest(unittest.TestCase):
    def setUp(self):
        os.environ['AMQP_HOSTNAME'] = AMQP_HOSTNAME

        self.__client = __main__.app.test_client()
        self.__client.testing = True

        self.__amqp_manager = AMQPManager(AMQP_HOSTNAME)
        self.__learner_tasks_queue_name = __main__.LEARNER_TASKS_QUEUE_NAME.format(job_name_=JOB_NAME)
        self.__filter_tasks_queue_name = __main__.FILTER_TASKS_QUEUE_NAME.format(job_name_=JOB_NAME)
        self.__fuser_tasks_queue_name = __main__.FUSER_TASKS_QUEUE_NAME.format(job_name_=JOB_NAME)

    def tearDown(self):
        pass
    
    def test_post_job(self):
        data = {
            'name': JOB_NAME,
            'dataset_name': DATASET_NAME,
            'training_rate': TRAINING_RATE,
            'fusion_rate': FUSION_RATE,
            'sample_rate': TRAINING_SAMPLE_RATE,
            'class_attribute': CLASS_ATTRIBUTE,
            'class_attribute_type': CLASS_ATTRIBUTE_TYPE,
            'true_class_value': TRUE_CLASS_VALUE,
            'include_attributes': INCLUDE_ATTRIBUTES,
            'exclude_attributes': EXCLUDE_ATTRIBUTES,
            'attributes_rate': ATTRIBUTES_RATE,
            'random_seed': RANDOM_SEED,
            'include_header': INCLUDE_HEADER,
            'duration': DURATION,
            'threshold': THRESHOLD,
            'learners_number': LEARNERS_NUMBER,
            'learner_parameters': LEARNER_PARAMETERS,
            'executor_parameters': EXECUTOR_PARAMETERS,
        }
        
        response = self.__client.post(
            '/job',
            data=data,
            content_type='multipart/form-data',
        )
        
        print(response.data)
        
        self.assertEqual(response.status_code, 200)

        self.__amqp_manager.delete_queue(self.__learner_tasks_queue_name)
        self.__amqp_manager.delete_queue(self.__filter_tasks_queue_name)
        self.__amqp_manager.delete_queue(self.__fuser_tasks_queue_name)
