test: version
	tox

version:
	cp version postgres_aas/version

clean:
	rm -rf dist postgres_aas.egg-info

distclean: clean
	rm -rf build postgres_aas/version .tox

package: version
	python setup.py sdist

.PHONY: clean package publish test version
