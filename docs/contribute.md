# Contribute

Récupérez les sources
```bash
$ git clone https://github.com/pprados/virtual_dataframe virtual_dataframe
```
puis:

```bash
$ cd virtual_dataframe
$ make configure
$ conda activate virtual_dataframe
$ make test
```

## Principes d'organisation du projet
- Un ``make validate`` est exécuter automatiquement avant un ``git push`` sur la branche ``master``.
  Il ne doit pas générer d'erreurs. Cela permet d'avoir un build incassable, en ne publiant
  que des commits validés.
  Il est possible de forcer tout de même le push avec ``FORCE=y git push``.
  Cela permet d'avoir l'équivalent d'une CI/CD en local. Bien entendu, cela peut etre supprimé
  si une plateforme d'intégration est disponible.

- La version du projet et déduite du tag courant GIT

- Le code du projet doit être présent dans le package ``virtual_dataframe``.
  Ils doivent être exécuté depuis le ``home`` du projet. Pour cela, une astuce consiste
  à forcer le ``home`` dans les templates Python de PyCharm. Ainsi, un ``run`` depuis un source
  fonctionne directement, sans manipulation.

- Le `typing <https://realpython.com/python-type-checking/>`_ est recommandé, afin d'améliorer la qualité du code
  et sa documentation. Vous pouvez vérifier cela avec ``make typing``, ou ajouter automatiquement une partie du typing
  à votre code avec ``make add-typing``.

## Truc et astuces
Quelques astuces disponibles dans le projet.

### Les test
Les tests sont divisés en deux groupes : ``unit-test`` et ``functional-test``.
Il est possible d'exécuter l'un des groups à la fois (``make (unit|functional)-test``) ou
l'ensemble (``make test``).

Les tests sont parallélisés lors de leurs executions. Cela permet de bénéficier des architectures
avec plusieurs coeurs CPU. Pour désactiver temporairement cette fonctionnalité, il suffit
d'indiquer un nombre de coeur réduit à utiliser. Par exemple : ``NPROC=1 make test``

### Vérifier le build
Pour vérifier que le ``Makefile`` est correct, vous pouvez vider l'environement conda avec ``make clean-venv``
puis lancer votre règle. Elle doit fonctionner directement et doit même pouvoir être exécuté deux fois
de suite, sans rejouer le traitement deux fois. Par exemple :

```bash
$ make validate
```

### Déverminer le Makefile
Il est possible de connaitre la valeur calculée d'une variable dans le ``Makefile``. Pour cela,
utilisez ``make dump-MA_VARIABLE``.

Pour comprendre les règles de dépendances justifiant un build, utilisez ``make --debug -n``.
{% if cookiecutter.use_jupyter == 'y' %}
### Convertir un notebook

Il est possible de convertir un notebook en script, puis de lui ajouter un typage.

```bash
make nb-convert add-typing
```

### Gestion des règles ne produisant pas de fichiers
Le code génère des fichiers  ``.make-<rule>`` pour les règles ne produisant pas
de fichier, comme ``test`` ou ``validate`` par exemple. Vous pouvez ajoutez ces
fichiers à GIT pour mémoriser la date de la dernière exécution. Ainsi, les
autres développeurs n'ont pas besoin de les ré-executer si ce n'est pas nécessaire.

{% endif %}

### Recommandations
- Utilisez un CHANGELOG basé sur `Keep a Changelog <https://keepachangelog.com/en/1.0.0/>`_,
- Utilisez un format de version conforme à `Semantic Versioning <https://semver.org/spec/v2.0.0.html>`_.
- Utiliser une approche `Develop/master branch <https://nvie.com/posts/a-successful-git-branching-model/>`_.
- Faite toujours un ``make validate`` avant de commiter le code
