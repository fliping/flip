./extention/mdb:
	git clone https://gitorious.org/mdb/mdb.git ./extention/mdb


all: ./extention/mdb
	git clone --recursive git@github.com:luvit/luvi.git ./.luvi
	make -C ./.luvi
	mv ./.luvi/build/luvi ./luvi
	git clone git@github.com:luvit/luvit.git --branch luvi-up ./.luvit
	LUVI_APP=./.luvit LUVI_TARGET=./luvit ./luvi
	LUVI_APP=./lib;./.luvit LUVI_TARGET=./flip-test ./luvit
	git submodule update --init --recursive
	make -C ./extention/mdb/libraries/liblmdb
