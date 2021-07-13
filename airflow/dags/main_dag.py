from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
import requests
import smtplib, ssl

the_key = "W3dEB3luQTF3eQkVjBzctrnJiTPAnwEm"

# functions


def fiction(**kwargs):

	fict_link = "https://api.nytimes.com/svc/books/v3/lists/current/combined-print-and-e-book-fiction/json?&api-key=" + the_key
	response = requests.get(fict_link)
	fic_json_data = response.json()
	task_instance = kwargs['task_instance']
	task_instance.xcom_push(key='fic_json_data', value=fic_json_data)
	return fic_json_data


def nonfiction(**kwargs):

	nonfict_link = "https://api.nytimes.com/svc/books/v3/lists/current/combined-print-and-e-book-nonfiction/json?&api-key=" + the_key
	response = requests.get(nonfict_link)
	nonfic_json_data = response.json()
	task_instance = kwargs['task_instance']
	task_instance.xcom_push(key='nonfic_json_data', value=nonfic_json_data)
	return nonfic_json_data

def clean_data(task_instance, **kwargs):
	# parse json responses of both, insert into lists

	fict_list = []
	nonfict_list = []

	fict = task_instance.xcom_pull(key='fic_json_data')
	nonfict = task_instance.xcom_pull(key='nonfic_json_data')

	pub_date = fict['results']['published_date']

	for i in fict['results']['books']:
		title = i['title'].split()
		title = [word.capitalize() for word in title]
		title = " ".join(title)
		fict_list.append(title)

	for i in nonfict['results']['books']:
		title = i['title'].split()
		title = [word.capitalize() for word in title]
		title = " ".join(title)
		nonfict_list.append(title)

	msg = "The Combined Print & E-Book Fiction Best Sellers list for the week of " + str(pub_date) + " is: " + str(fict_list) + "\n\n" + f"The Combined Print & E-Book Non-Fiction Best Sellers list for the week of {pub_date} is: " + str(nonfict_list)

	task_instance.xcom_push(key='msg', value=msg)

	return msg

# def email(task_instance, **kwargs):

# 	msg = task_instance.xcom_pull(key='msg')

# 	send_email(
# 		to=["rinaldinick88@gmail.com", "edrinaldi88@gmail.com"],
# 		subject="This week's NYTimes Best Seller List",
# 		html_content=msg,
# 		)
	
	
def send_emails(task_instance, **kwargs):

	# task_instance = kwargs['task_instance']
	msg = task_instance.xcom_pull(key='msg')

	port = 465
	password="Justforfun7/"
	sender_email="rinalditesting88@gmail.com"
	smtp_server="smtp.gmail.com"
	recipients=["rinaldinick88@gmail.com", "edrinaldi88@gmail.com"]

	context = ssl.create_default_context()

	with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
		server.login(sender_email, password)
		server.sendmail(sender_email, recipients, msg)



default_args = {
	'owner': 'admin',
	'email': ['rinaldinick88@gmail.com'],
	'retries': 2,
}

# utilize context manager
with DAG('nytbook_dag1',
		default_args=default_args,
		description='A simple NYTimes Best Sellers DAG',
		schedule_interval="@daily",
		catchup=False,
		start_date = days_ago(2),
		) as dag:

	t0 = PythonOperator(
		task_id='fetch_fiction',
		python_callable=fiction,
		op_kwargs={'key':the_key}
	)

	t1 = PythonOperator(
		task_id='fetch_nonfiction',
		python_callable=nonfiction,
		op_kwargs={'key':the_key}
	)

	t2 = PythonOperator(
		task_id='clean_data',
		python_callable=clean_data
	)

	t3 = PythonOperator(
		task_id='send_results',
		python_callable=send_emails,
		)

	t0 >> t1 >> t2 >> t3
