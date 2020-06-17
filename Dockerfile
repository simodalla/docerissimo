FROM python:2.7

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /code

# MYSQL
RUN wget -O /mysql-connector-python.deb \
 https://downloads.mysql.com/archives/get/p/29/file/mysql-connector-python_1.2.3-1debian6.0_all.deb \
 && dpkg -i /mysql-connector-python.deb
RUN cp -R /usr/share/pyshared/mysql /usr/local/lib/python2.7/site-packages
RUN cp /usr/share/pyshared/mysql_connector_python-1.2.3.egg-info /usr/local/lib/python2.7/site-packages

# ORACLE

RUN mkdir -p /opt/oracle/ \
    && wget -O /instaclient.zip https://download.oracle.com/otn_software/linux/instantclient/19600/instantclient-basic-linux.x64-19.6.0.0.0dbru.zip
RUN unzip /instaclient.zip -d /opt/oracle/ \
    && mv /opt/oracle/instantclient_19_6 /opt/oracle/instantclient
RUN pip install --upgrade cx_Oracle
RUN echo "deb http://deb.debian.org/debian buster contrib non-free" >> /etc/apt/sources.list
RUN apt-get update && apt-get install -y libaio-dev libaio1
RUN echo /opt/oracle/instantclient > /etc/ld.so.conf.d/oracle-instantclient.conf
RUN ldconfig
COPY ./tnsnames.ora /opt/oracle/instantclient/network/admin/

COPY . /code/
