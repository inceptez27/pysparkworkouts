spark-build:
		mkdir -p ./deploy
		cp driver.py deploy
		cp /home/hduser/install/mysql-connector-java.jar deploy
		zip -r deploy/src.zip src