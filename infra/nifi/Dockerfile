FROM apache/nifi:1.22.0
RUN wget https://cdn.mysql.com/archives/mysql-connector-java-8.0/mysql-connector-j-8.0.33.tar.gz && \
	tar -xf mysql-connector-j-8.0.33.tar.gz && \
	cp mysql-connector-j-8.0.33/mysql-connector-j-8.0.33.jar /opt/nifi/nifi-current/lib && \
	wget https://jdbc.postgresql.org/download/postgresql-42.7.4.jar && \
	cp postgresql-42.7.4.jar /opt/nifi/nifi-current/lib && \
	rm -rf mysql-connector* postgresql-42.7.4.jar