FROM smadnet/base

#? Install necessary packages for the module

#? Copy files to docker
ADD . /module
RUN rm /module/config.json

#? Navigate to working directory
WORKDIR /module

#? Install requirements python libraries for the module
RUN pip3 install -r requirements.txt

#? Then run the module
CMD ["python3", "-u", "/module/run.py"]