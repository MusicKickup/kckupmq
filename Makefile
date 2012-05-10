compile:
	rm -fR ./tmp-js
	mkdir -p ./tmp-js/lib
	./node_modules/.bin/coffee -c -o tmp-js/lib lib/*.coffee
 
test: compile
	./node_modules/.bin/vows test/*.coffee --spec
	./node_modules/.bin/jshint --config .jshintrc tmp-js/