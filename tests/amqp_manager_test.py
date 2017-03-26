import unittest
import time

from scheduler.amqp_manager import AMQPManager

AMQP_HOSTNAME = 'localhost'

QUEUE_NAME = 'gpfunction'

TASKS = [
    {
        'job_name': 'gpfunction',
        'task_number': 0,
        'dataset_name': 'higgs',
        'training_rate': 0.5,
        'fusion_rate': 0.3,
        'sample_rate': 0.1,
        'class_attribute': 'label',
        'include_attributes': [],
        'exclude_attributes': [],
        'attributes_rate': 0.5,
        'random_seed': 0,
        'duration': 60,
        'learner_parameters': {
            'xover_op': 'SPUCrossover',
            'pop_size': 1000,
            'mutation_rate': 0.1,
        },
    },
    {
        'job_name': 'gpfunction',
        'task_number': 1,
        'dataset_name': 'higgs',
        'training_rate': 0.5,
        'fusion_rate': 0.3,
        'sample_rate': 0.1,
        'class_attribute': 'label',
        'include_attributes': [],
        'exclude_attributes': [],
        'attributes_rate': 0.5,
        'random_seed': 0,
        'duration': 60,
        'learner_parameters': {
            'xover_op': 'KozaCrossover',
            'pop_size': 2000,
            'mutation_rate': 0.9,
        },
    },
]


class AMQPManagerTest(unittest.TestCase):
    def setUp(self):
        self.__amqp_manager = AMQPManager(AMQP_HOSTNAME)
    
    def tearDown(self):
        self.__amqp_manager.close()

    def test_create_queue(self):
        self.__amqp_manager.create_queue(QUEUE_NAME)
        self.assertTrue(self.__amqp_manager.queue_exists(QUEUE_NAME))

        self.__amqp_manager.delete_queue(QUEUE_NAME)
        self.assertFalse(self.__amqp_manager.queue_exists(QUEUE_NAME))

    def test_publish_messages(self):
        self.__amqp_manager.create_queue(QUEUE_NAME)

        self.__amqp_manager.publish_messages(QUEUE_NAME, TASKS)

        time.sleep(1)

        self.assertEqual(self.__amqp_manager.queue_size(QUEUE_NAME), len(TASKS))

        self.__amqp_manager.delete_queue(QUEUE_NAME)

    def test_consume_messages(self):
        self.__amqp_manager.create_queue(QUEUE_NAME)

        self.__amqp_manager.publish_messages(QUEUE_NAME, TASKS)

        consumed_tasks, delivery_tags = self.__amqp_manager.consume_messages(QUEUE_NAME, len(TASKS))

        for i in range(len(TASKS)):
            print(consumed_tasks[i])
            self.assertEqual(consumed_tasks[i], TASKS[i])

        self.__amqp_manager.acknowledge_messages(QUEUE_NAME, delivery_tags)

        self.__amqp_manager.delete_queue(QUEUE_NAME)
