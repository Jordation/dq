cons:
	go build ./cli/ .
	./cli/cli.exe 3030 c2
srv:
	go build ./cli/ .
	./cli/cli.exe 3030 s2 
