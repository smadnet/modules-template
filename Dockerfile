FROM smadnet/base

#? Install necessary packages for the module

#? Copy files to docker
ADD . /module
WORKDIR /module

#? Install requirements python libraries for the module
RUN pip3 install -r requirements.txt

#? Then run the module
CMD ["python3", "-u", "run.py"]