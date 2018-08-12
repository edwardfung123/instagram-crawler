import datetime

from flask import Flask, render_template

app = Flask(__name__)

import logging
logging.basicConfig(level=logging.DEBUG)

datastore_client = None

def get_datastore_client():
    global datastore_client
    if datastore_client is None:
        from google.cloud import datastore
        datastore_client = datastore.Client()
    return datastore_client

pubsub_publisher_client = None
pubsub_subscriber_client = None

def get_pubsub_publisher_client():
  global pubsub_publisher_client
  if pubsub_publisher_client is None:
    from google.cloud import pubsub
    pubsub_publisher_client = pubsub.PublisherClient()
  return pubsub_publisher_client


def get_pubsub_subscriber_client():
  global pubsub_subscriber_client
  if pubsub_subscriber_client is None:
    from google.cloud import pubsub
    pubsub_subscriber_client = pubsub.SubscriberClient()
  return pubsub_subscriber_client


@app.route('/')
def root():
    from flask import request
    message = request.args.get('message')
    return render_template('index.html', message=message)


@app.route('/apis/enqueue_userid', methods=['POST'])
def enqueue_userid():
    from flask import abort, redirect, request
    userid = request.form.get('userid')
    if not userid:
      abort(401, 'Missing userid')

    datastore_client = get_datastore_client()
    user_key = datastore_client.key('ig_user', userid)
    user = datastore_client.get(user_key)
    if user is None:
      logging.debug('No user is found, create a new one')
      from google.cloud import datastore
      user = datastore.Entity(user_key)
    from datetime import datetime
    user['last_enqueue'] = datetime.utcnow()
    datastore_client.put(user)
    return redirect('/?message=enqueued+{}'.format(userid))

if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    # Flask's development server will automatically serve static files in
    # the "static" directory. See:
    # http://flask.pocoo.org/docs/1.0/quickstart/#static-files. Once deployed,
    # App Engine itself will serve those files as configured in app.yaml.
    import os
    key_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'datastore_local_dev_service_account.json')
    logging.debug(key_path)

    from google.cloud import datastore
    datastore_client = datastore.Client.from_service_account_json(key_path)

    # The pubsub lib is bugged. It throws error when loading from service account.
    #from google.cloud import pubsub
    #logging.debug(dir(pubsub.PublisherClient))
    #pubsub_publisher_client = pubsub.PublisherClient.from_service_account_json(key_path)
    #pubsub_subscriber_client = pubsub.SubscriberClient.from_service_account_json(key_path)

    app.run(host='127.0.0.1', port=8080, debug=True)
