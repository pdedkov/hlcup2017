FROM golang:1.8

MAINTAINER Pavel E. Dedkov <pavel.dedkov@gmail.com>

WORKDIR /go/src/app
COPY . .

RUN go-wrapper download   # "go get -d -v ./..."
RUN go-wrapper install    # "go install -v ./..."

EXPOSE 80
CMD ["go-wrapper", "run"] # ["app"] 
