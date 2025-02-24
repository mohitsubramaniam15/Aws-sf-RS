sudo apt update
sudo apt install python3-pip
sudo apt install python3.10-venv
python3 -m venv redfin_venv
source redfin_venv/bin/activate
pip install pandas
pip install boto3
pip install --upgrade awscli
pip install apache-airflow
airflow version
python3 --version
aws configure
airflow standalone
