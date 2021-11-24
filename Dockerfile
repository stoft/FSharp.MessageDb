FROM alpine:3.14

RUN apk add --no-cache bash git openssh 
RUN apk add --no-cache postgresql

RUN git clone https://github.com/message-db/message-db.git
WORKDIR /message-db/database
RUN chmod +x install.sh

ENTRYPOINT [ "./install.sh" ]