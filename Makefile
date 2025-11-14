deploy:
	lein deploy clojars

test:
	bin/kaocha

.PHONY: test deploy
