from datetime import datetime

with open("../data/postgres_updated.txt", "w") as arquivo:
            arquivo.write(str(datetime.now().strftime("%H")))