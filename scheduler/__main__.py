import json
import os
import ast

import flask

from scheduler import amqp_manager
from scheduler.tasks_generators.learner_tasks_generator import LearnerTasksGenerator
from scheduler.tasks_generators.filter_tasks_generator import FilterTasksGenerator
from scheduler.tasks_generators.fuser_tasks_generator import FuserTasksGenerator


LEARNER_TASKS_QUEUE_NAME = '{job_name_}@learner.tasks'
FILTER_TASKS_QUEUE_NAME = '{job_name_}@filter.tasks'
FUSER_TASKS_QUEUE_NAME = '{job_name_}@fuser.tasks'


# App initialization.
app = flask.Flask(__name__)


def get_amqp_manager():
    if not hasattr(flask.g, 'amqp_manager'):
        flask.g.amqp_manager = amqp_manager.AMQPManager(
            hostname=os.environ.get('AMQP_HOSTNAME', 'rabbitmq'),
        )
    return flask.g.amqp_manager


@app.teardown_appcontext
def delete_amqp_manager(exception):
    if hasattr(flask.g, 'amqp_manager'):
        flask.g.amqp_manager.close()


@app.route('/job', methods=['POST'])
def post_job():
    """
    Posts a new job in the system.
    POST: /job

    :param name: the name of the job
    :type name: str

    :param form['dataset_name']: the name of the dataset
    :type form['dataset_name']: str

    :param form['training_rate']: the percentage of the dataset to consider as training split
    :type form['training_rate']: float

    :param form['fusion_rate']: the percentage of the dataset to consider as fusion split
    :type form['fusion_rate']: float

    :param form['sample_rate']: the percentage of instances, within the training split, to include
    :type form['sample_rate']: float

    :param form['class_attribute']: the name of the class attribute
    :type form['class_attribute']: str

    :param form['class_attribute_type']: the type of the class attribute, 'integer' | 'real' | 'text'
    :type form['class_attribute_type']: str

    :param form['true_class_value']: the class value considered as true
    :type form['true_class_value']: object

    :param form['include_attributes']: the list of the attributes to include, None otherwise
    :type form['include_attributes']: list[str]

    :param form['exclude_attributes']: the list of the attributes to exclude, None otherwise
    :type form['exclude_attributes']: list[str]

    :param form['attributes_rate']: the percentage of attributes to include
    :type form['attributes_rate']: float

    :param form['random_seed']: the random seed
    :type form['random_seed']: int

    :param form['include_header']: if True, it includes the header in the output CSV
    :type form['include_header']: bool

    :param form['duration']: the duration in seconds
    :type form['duration']: int

    :param form['threshold']: the threshold to apply the filter policy
    :type form['threshold']: float

    :param form['learners_number']: the total number of learners.
    :type form['learners_number']: int

    :param form['learn_parameters']: the list of parameters for the learner,
     in the form {'name': str, 'type': 'integer' | 'real' | 'text', 'values': list[object],
     'range': {'start': int, 'stop': int, 'step': int}}
    :type form['learn_parameters']: list[object]

    :param form['predict_parameters']: the list of parameters for the executor,
    in the form {'name': str, 'type': 'integer' | 'real' | 'text', 'value': object}
    :type form['predict_parameters']: list[object]
    """
    amqp_manager = get_amqp_manager()

    name = flask.request.form.get('name')

    dataset_name = flask.request.form.get('dataset_name')
    training_rate = float(flask.request.form.get('training_rate'))
    fusion_rate = float(flask.request.form.get('fusion_rate'))
    sample_rate = float(flask.request.form.get('sample_rate'))
    class_attribute = flask.request.form.get('class_attribute')
    class_attribute_type = flask.request.form.get('class_attribute_type')
    true_class_value = flask.request.form.get('true_class_value')
    include_attributes = flask.request.form.getlist('include_attributes')
    exclude_attributes = flask.request.form.getlist('exclude_attributes')
    attributes_rate = float(flask.request.form.get('attributes_rate'))
    random_seed = int(flask.request.form.get('random_seed'))
    include_header = ast.literal_eval(flask.request.form.get('include_header'))

    duration = int(flask.request.form.get('duration'))
    threshold = float(flask.request.form.get('threshold'))

    learners_number = int(flask.request.form.get('learners_number'))
    learn_parameters = json.loads(flask.request.form.get('learn_parameters'))

    predict_parameters = json.loads(flask.request.form.get('predict_parameters'))

    # Retrieves the queues names.
    learner_tasks_queue_name = LEARNER_TASKS_QUEUE_NAME.format(job_name_=name)
    filter_tasks_queue_name = FILTER_TASKS_QUEUE_NAME.format(job_name_=name)
    fuser_tasks_queue_name = FUSER_TASKS_QUEUE_NAME.format(job_name_=name)

    # Prepares the queues.
    amqp_manager.create_queue(learner_tasks_queue_name)
    amqp_manager.create_queue(filter_tasks_queue_name)
    amqp_manager.create_queue(fuser_tasks_queue_name)

    # Generates and publishes the learner tasks.
    learner_tasks = LearnerTasksGenerator(
        job_name=name,
        tasks_number=learners_number,
        dataset_name=dataset_name,
        training_rate=training_rate,
        fusion_rate=fusion_rate,
        sample_rate=sample_rate,
        class_attribute=class_attribute,
        class_attribute_type=class_attribute_type,
        true_class_value=true_class_value,
        include_attributes=include_attributes,
        exclude_attributes=exclude_attributes,
        attributes_rate=attributes_rate,
        random_seed=random_seed,
        include_header=include_header,
        duration=duration,
        learn_parameters=learn_parameters,
    )
    amqp_manager.publish_messages(learner_tasks_queue_name, learner_tasks)

    # Generates and publishes the filter task.
    filter_tasks = FilterTasksGenerator(
        job_name=name,
        learner_outputs_number=learners_number,
        dataset_name=dataset_name,
        training_rate=training_rate,
        fusion_rate=fusion_rate,
        class_attribute=class_attribute,
        class_attribute_type=class_attribute_type,
        true_class_value=true_class_value,
        include_attributes=include_attributes,
        exclude_attributes=exclude_attributes,
        attributes_rate=attributes_rate,
        random_seed=random_seed,
        include_header=include_header,
        threshold=threshold,
        predict_parameters=predict_parameters,
    )
    amqp_manager.publish_messages(filter_tasks_queue_name, filter_tasks)

    # Generates and publishes the fuser task.
    fuser_tasks = FuserTasksGenerator(
        job_name=name,
        dataset_name=dataset_name,
        training_rate=training_rate,
        fusion_rate=fusion_rate,
        class_attribute=class_attribute,
        class_attribute_type=class_attribute_type,
        true_class_value=true_class_value,
        include_attributes=include_attributes,
        exclude_attributes=exclude_attributes,
        attributes_rate=attributes_rate,
        random_seed=random_seed,
        include_header=include_header,
        predict_parameters=predict_parameters,
    )
    amqp_manager.publish_messages(fuser_tasks_queue_name, fuser_tasks)

    return 'Job created correctly.'


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False)
