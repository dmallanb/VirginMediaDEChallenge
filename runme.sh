echo "--------------------------------------
Installing necessary python packages..
--------------------------------------"
pip install -r requirements.txt | pip3 install -r requirements.txt
echo "--------------------------------------
Running Unit Tests
--------------------------------------"
python -m unittest -v | python3 -m unittest -v
echo "--------------------------------------
Running pipeline
--------------------------------------"
python main.py | python3 main.py