# Introducción a DataBricks e ingeniería de características

Repositorio del taller Introducción a DataBricks e ingeniería de características con PySpark.

## Introducción

Este taller tiene como mira dar a conocer Databricks como uno de los entornos interactivos basados en IPython así como podamos conocer algunas ideas que nos llevarán a tener una guía para moldear estrategias que nos permitan lograr un estilo de programación flexible, compatible, legible y con la posibilidad de escalar el desarrollo.

Databricks es una herramienta que mal usada puede ser un dolor de cabeza pero bien usada simplemente será otro medio para llegar a la aportación de valor que buscamos para dar tanto a nuestros equipos como a las personas que consultarán y harán uso de esa información.

## Motivación

El entorno interactivo es algo que ha adquirido mucha relevancia recientemente y como tal es importante conocer algunas de las características que más resaltan de este para poder hacer un correcto uso del recurso así como tomar sus beneficios y evitar caer en algunos de los puntos débiles que pudieran traernos. Ya que a diferencia de un proceso que ejecuta en secuencia, algunos descuidos nos pueden llevar a obtener los resultados buscados en una ocasión y podamos perderlos posteriormente.

## Temas

A continuación veremos una lista de los temas que veremos:

1. [Entornos de desarrollo interactivo basados en IPython](#entornos-de-desarrollo-interactivo-basados-en-ipython)
2. [Magics y uso de comandos especiales](#magics-y-uso-de-comandos-especiales)
3. [Consideraciones y riesgos para entornos de desarrollo interactivo](#consideraciones-y-riesgos-para-entornos-de-desarrollo-interactivo)
4. [Paradigma funcional, guía de estilos y buenas prácticas para redacción de algoritmos](#paradigma-funcional-guía-de-estilos-y-buenas-prácticas-para-redacción-de-algoritmos)
5. [Ingeniería de características con PySpark](#ingeniería-de-características-con-pyspark)
6. [Consideraciones importantes respecto al flujo de desarrollo y la planificación de actividades](#consideraciones-importantes-respecto-al-flujo-de-desarrollo-y-la-planificación-de-actividades)

## Entornos de desarrollo interactivo basados en IPython

El desarrollo interactivo en los años recientes ha sido una herramienta con grandes ventajas y aportaciones en el desarrollo de ideas y productos tanto de software como analítica.

En cuanto a los entornos interactivos que existen, entre los más populares son los basados en IPython. Esto se debe al rol que ha ido encontrando Python en el mundo del desarrollo así como una herramienta muy importante que ha crecido junto con IPython: *Jupyter*.

Algunos otros entornos interactivos son:

- Mathematica
- Matlab
- Maple
- SageMath
- Línea de comandos de UNIX
- Ruby
- Prolog
- Julia

### SageMath

El entorno de SageMath tiene su primer lanzamiento el 24 de Febrero de 2005 y este presenta una colección de herramientas de código abierto para matemáticas bajo una misma interfaz de Python y Cython.

Con una similaridad con los cuadernos de Mathematica, SageMath desarrolla una interfaz de cuadernos interactivos con formato de celdas que permiten el uso de su sistema CAS, visualización de gráficos y dibujado de expresiones con LaTeX.

Eventualmente SageMath dejó de utilizar su interfaz de cuadernos interactivos para optar por el uso de los cuadernos de Jupyter.

### Jupyter

Para 2014 el creador de IPython anunció un nuevo proyecto llamado *Jupyter*. Mientras que IPython continuó existiendo como el intérprete interactivo de Python, también empezó a existir como núcleo para Jupyter. Por otro lado, otros elementos como los cuadernos y algunos componentes agnósticos de IPython migraron para ser parte de Jupyter. De forma adicional podemos notar que Jupyter es agnóstico de lenguajes de programación y más bien su nombre puede referenciarse con otros lenguajes de programación como son Julia, Scala, R, C++, Javascript, Spark, entre otros.

### Databricks

Databricks es una empresa americana de software que fue fundada por los creadores de Apache Spark y esta provee herramientas para trabajar y desarrollar con Spark, así como gestión automática de clusters y cuadernos de interactivos del estilo IPython.

Databricks es una plataforma que se encuentra disponible para múltiples nubes de cómputo famosas como son Google Cloud Platform (GCP), Azure y AWS (Amazon Web Services).

## Magics y uso de comandos especiales

Los entornos interactivos basados en IPython tienen una serie de comandos especiales llamados *magics* y estos podemos reconocerlos ya que comienzan con el caracter `%`. En IPython, están los llamados *sobre la lînea* o *line magics* así como los *sobre la celda* o *cell magics*. Dependiendo de las necesidades es si se optará por una u otra. La lista de *magics* para IPython se puede encontrar [aquí](https://ipython.readthedocs.io/en/stable/interactive/magics.html).

### Sobre la línea o *line magics*

Los magics de línea corresponden a comandos para ejecutar en una línea. Estos suelen ser más rápidos de ejecución como sería el caso de `%timeit`. Estos se mandan llamar con un sólo `%`.

### Sobre la celda o *cell magics*

Los magics de celda corresponden a comandos que ejecutarán para toda una celda, incluyendo todas las instrucciones que puedan estar en esta. Estos se mandan llamar con un doble `%%`.

### Algunos magics populares

Algunos de los magics más populares son:

- `%run`: Ejecución de un archivo externo de Python.
- `%load`: Cargar el código de un archivo a la sesión actual de IPython.
- `%timeit`: Permite medir el tiempo de ejecución de una línea o celda dependiendo del modo con el que es usado.
- `%pip`: Ejecuta el manejador de paquetes Pip para gestionar paquetes (usualmente instalar).
- `%%html`: Permite cambiar el formato de salida del cuaderno que corresponde con código `html`.
- `%reset`: Borra las variables y métodos almacenados en la memoria.

## Consideraciones y riesgos para entornos de desarrollo interactivo

Los entornos interactivos vinieron a proporcionar una forma diferente de realizar las cosas. Comenzando por el hecho de cambiar la forma en que usualmente se programaba ya que los programas solían ser hechos de principio a fin.

Ahora con los entornos de desarrollo interactivo es posible ir desarrollando el programa así como ir realizando consultas en base a las diferentes propiedades y características que podemos extraer de la información almacenada en las variables.

Una de las propiedades muy interesantes es la parte interactiva del proceso en como el usuario puede no solamente operar la información en memoria o como bytes sino incluso dar sentido por medio de otras estructuras matemáticas como podrán ser gráfos así como el uso de diferentes modelos para poder estudiar de manera más específica la representación del mundo en estos datos.

Así como en Mathematica en su momento y como podíamos encontrar simplificaciones o reducciones algebráicas en expresiones para después graficar en 2, 3 o 4 dimensiones, ahora podemos alimentar a diferentes procesos, objetos y modelos con estos datos para buscar y explorar por las respuestas que buscamos.

Sin embargo no todo es perfecto y hay detalles que debemos tener presentes para eviarnos problemas con los entornos de desarrollo interactivo. Comenzando por tener consciencia de la secuenciación de nuestras instrucciones. Así como de la mutabilidad que ocurrirá en nuestras variables y el orden en que ocurren estas mutaciones para llegar a lo que buscamos.

Algunos de los riesgos que nos pueden perjudicar durante el desarrollo interactivo es el no llevar un orden de nuestras estructuras de datos así como variables. Ya sea que en una celda hagamos uso de un recurso que aún no ha sido definido, o que al operar otra celda, no consideremos que puede estar mutando un objeto que no tenemos en mente.

Otros puntos débiles del entorno de desarrollo interactivo es que puede requerir más recursos para ejecutar a diferencia de archivos con un algoritmo específico para realizar alguna actividad particular en base a parámetros que recibe. Y también cabe mencionar que si los programas no presentan compatibilidad con IPython, no será posible ejecutarlos en entornos interactivos basados en IPython.

Sin olvidar el tema de la seguridad, ya que al estar estos cuadernos en interfaces web, pueden estar expuestos a ataques que comprometan la seguridad como pueden ser ataques XSS (cross-site scripting). Si hay información sensible para ser procesada, quizá lo mejor sea buscar ejecutar estos entornos en navegadores aislados y completamente enfocados en realizar desarrollo interactivo.

## Paradigma funcional, guía de estilos y buenas prácticas para redacción de algoritmos

### Paradigma Funcional

Esta es una forma o estilo de programación que se enfoca en mirar a la computación como si se tratase de la evaluación de funciones matemáticas *puras*. Algunos puntos a considerar son los siguientes:

1. Inmutabilidad: Los valores no pueden modificarse una vez definidos. Esto lleva a una programación más segura y predecible.
2. Funciones Puras: Las funciones no tienen efectos secundarios y siempre devuelven el mismo resultado dado un mismo argumento.
3. Evaluación Perezosa: Las funciones sólo se evaluan cuando es necesario.
4. Composición de Funciones: Las funciones son consideradas como elementos de *primera clase* con lo cual significa que estas pueden ser asociadas a variables y evaluadas dentro de funciones así como devueltas como resultados.
5. Eliminación de pasos intermedios innecesarios y un mejor manejo de memoria debido a que los entornos o contextos locales son vaciados de la memoria una vez terminada la función.

Todo esto permite que el paradigma funcional sea adecuado para desarrollar programas concurrentes y distribuidos, facilitando la modularización, escalabilidad, y la reducción de errores.

### Guía de Estilos

Las guías de estilo son convenciones que establecen estilos y formatos a seguir para la escritura de código fuente buscando como meta una forma consistente en la redacción y facilidad para leer. Algunos puntos claves son los siguientes:

1. Nomenclatura: Cómo nombrar variables, funciones, clases, etc.
2. Sangría: La cantidad de espacios o tabuladores que serán usados para dar sangría a los bloques en el algoritmo.
3. Comentarios: Como y cuando escribir comentarios para documentar el algoritmo.
4. Espacio en blanco: Cómo utilizar el espacio en blanco para separar elementos y hacer del algoritmo más legible.
5. Longitud de líneas: La lóngitud máxima permitida de caracteres para cada línea.
6. Convenciones por lenguaje de programación: Convenciones para llevar un estilo de acuerdo a la estructura y diseño de cada lenguaje de programación.

Cabe mencionar que es de gran importancia (aunque algunas veces pueda parecer que casi nadie lo hace) seguir una guía de estilo en prioridad de:

1. Las convenciones que tenga el equipo.
2. Las convenciones que tenga el área a la que se pertenece.
3. La guía de estilo propia del lenguaje de programación.

Esto con la finalidad de buscar mantener los algoritmos organizados, legibles y fáciles de mantener. Un incentivo es que esto en el largo plazo ayuda a reducir la posibilidad de errores y problemas en el futuro.

Como nota adicional y hablando desde la experiencia: Esta es una característica muy valorada pero con poca popularidad. Hacer un hábito de escribir con guía de estilo pronto se vuelve parte de tí y habla mucho del legado que dejas al haber participado en un proyecto.

### Buenas Prácticas para Redacción de Algoritmos

La redacción de algoritmos es algo que va más allá de simplemente *codificar* o escribir código fuente. Así como una novela está hecha de letras, que a su vez componen palabras, de igual forma la redacción de algoritmos es más que simplemente codificar. Ya que se trabajan ideas, y como se busca ejecutar y realizar estas tareas de la mejor forma, con mayor legibilidad, buscando ser óptima y legible, al mismo tiempo que pueda contarte la historia de como estará procesando la información y de forma adicional, pueda proveer menos complicaciones a la hora de implementar nuevas ideas a *esta historia*.

En este punto podemos intuir que para llevar a cabo buenas prácticas y una redacción sobresaliente así como buscar establecer los pilares para un desarrollo escalable y busque responder a las atenciones de negocio, es justamente hacer uso de lo que hemos visto:

- Organizar tus ideas en funciones para buscar modularizar, y tener control sobre los resultados generados.
- El punto anterior nos lleva a que si es posible generar una biblioteca para reciclar instrucciones y algoritmos, se busque impulsar. Esto ya que una vez se tienen funciones que realizan las tareas de forma sistemática y se hace uso de estas, si es necesario optimizar una de estas funciones, por consecuencia todo el proyecto se actualiza y es menos necesario indagar en todo el desarrollo para buscar los puntos de mejora.
- Guía de estilos: esto ayuda a organizar muy bien las ideas y si varias personas son capaces de organizar sus ideas de la misma forma, el apoyo y sinergia del equipo se vuelve mejor. Imaginen que cada quien hablase el Español o Inglés como quisiera. Si así fuera eventualmente perderíamos la capacidad de comunicar ideas complejas. De la misma forma, aunque la máquina entienda *el código*, al final las personas son quienes lo crean y los compiladores son los encargados de generar el código máquina correspondiente.
- Hacer correcto uso de los recursos computacionales: pensar que ni toda la memoria estará disponible para nuestro uso, así como ser responsables por la cantidad de recursos que ocupamos. Ya que un sistema caido por poca optimización de algoritmos, es una mala noticia por al menos dos puntos importantes: una cuenta alta en el uso de recursos desperdiciados, y una productividad perdida por la inaccesibilidad del recurso.

## Ingeniería de características con PySpark

La ingeniería de características consiste en el trabajo organizado y con finalidad de limpiar y estandarizar los datos para orientar la información en virtud de los modelos que vayamos a utilizar para sacar conocimiento de estas.

Algunos ejemplos de tareas en ingenería de características serîan:

- Tratamiento de datos nulos.
- Extracción de atributos.
- Preprocesamiento de datos en base a métricas y necesidades.

Algunas de las ventajas de estas prácticas son:

- Preparar los datos para un procesamiento más adecuado por parte de modelos u otros procesos.
- La facilidad de analizar esta información adicional previo a procesada por otros algoritmos, facilitando el análisis sobre los datos a explotar.

### Spark

Spark es una plataforma para el procesamiento masivo y distribuido de datos con la finalidad de realizar análisis de datos. Es eficiente, escalable y rápido. Algunas características de Spark son:

1. Escalabilidad: Por la naturaleza del lenguaje de programación que fue usado para construirlo, y construcción, permite que Spark pueda ser usado en clusters de procesamiento pudiendo así manejar grandes cantidades de datos.
2. Velocidad: Spark hace uso de memoria intermedia para almacenar datos por lo que al reducir su impacto en almacenamientos físicos, sus operaciones son más veloces.
3. APIs: Spark cuenta con un API para poder ser utilizado por otros lenguajes de programación como serían Java, Scala, Python y R entre otros menos conocidos.
4. Integración con otras tecnologías: Spark tiene la capacidad de integrarse con otras tecnologías enfocadas para el análisis de datos como serían Hadoop, Hive y Cassandra.
5. Amplia comunidad y soporte: Dado que Spark es código abierto, este cuenta con una amplia comunidad de usuarios y desarrolladores. Esto tiene como consecuencia una gran cantidad de recursos y soporte disponibles.

### PySpark

Por otro lado, PySpark es el API de Spark para Python sobre la cual podemos hacer uso de Spark a través de Python. En gran medida, si se conoce Spark, usar PySpark es transparente ya que las funciones, estructuras de datos y lógica se mantiene.

Algunos puntos claves a considerar son:

- Recordar que Python no fue construido con la idea de procesamiento paralelo o procesamiento de múltiples hilos en mente mientras que Spark sí lo es. Por lo que es importante considerar que al usar PySpark, busquemos hacer uso de las buenas prácticas de Spark para que esta plataforma se encargue de realizar los procesamientos, análisis, balanceos y demás que realiza por su lado para obtener los resultados que buscamos.
- Evitar hacer uso de métodos para recolección de datos como `collect()` o `toPandas()` (entre otros) ya que estos podrían saturar la memoria de los nodos principales volviendo inutilizable el cluster de procesamiento.
- Usar sabiamente el cluster de Spark buscando minimizar el uso de operaciones que copian las tablas enteras (muchas veces es posible generar una consulta de forma dinámica que nos permitirá evaluar con la función `expr()` y así minimizamos la cantidad de `withColumn()` que construye toda una copia de la tabla).

### Usando todo lo anterior

Procederemos a ver un ejercicio simple en Databricks que previamente fue generado en Jupyter dentro del editor VSCode. Este cuaderno está disponible con el nombre `Course.ipynb` en este mismo repositorio.

## Consideraciones importantes respecto al flujo de desarrollo y la planificación de actividades

Ahora incluyendo un poquito de temas que pueden ser más del área de metodologías y marcos de trabajo como scrum o agile, hay puntos importantes que como programadores debemos tener presentes para proteger nuestro recurso más valioso que tenemos: *el tiempo*.

### Equipos de trabajo ágiles

En los equipos de trabajo scrum, existe una forma de trabajar sobre iteraciones (o sprints) realizando planeaciones de las actividades como historias de usuarios y tareas. Las personas responsables de estas actividades son las figuras de *scrum master* y *product owner*. Posteriormente quienes ejecutarán estas tareas para realizarlas y a través de su realización aportar valor y volver el producto una realidad, son los *miembros del equipo*.

### Mi rol en un equipo de desarrollo como programador o analista

Como miembros del equipo, la gestión y manejo de las tareas para lograr el entregable no es parte de nuestra responsabilidad de forma directa. Por lo que nuestra responsabilidad como miembros del equipo es enfocarnos en construir el entregable.

### Peticiones ajenas

Algunas veces equipos ajenos al nuestro como podrían ser los equipos de negocio se acercan con peticiones que de primera mano podrían parecer simples e incluso sensatas. Sin embargo lo que tenemos que tener presente cuando trabajamos bajo estas metodologías o marcos de trabajo, es que recibir peticiones por parte de una figura ajena a los roles como product owner y scrum master puede generar compromisos que afecten nuestra agenda en relación a las historias de usuario o tareas, así como peudan afectar al equipo completo.

### ¿Cómo lidear con las peticiones ajenas?

El primer paso como miembros del equipo es apuntar rechazar dichas peticiones cuando se salen de la mira de la historia de usuario y redirigir con las figuras de scrum master y product owner. La finalidad de esta acción es que dichas peticiones sean consideradas dentro de la lista de actividades para lograr el entregable, o *backlog* y así al llegar una nueva planificación de iteración o sprint, puedan definirse las historias de usuario necesarias para lograr esa parte de la petición.

No hacerlo de esta forma puede llevarnos al incumplimiento de historias de usuario y posteriormente a que se llame la atención por no haber logrado los objetivos que estaban en mira para ser alcanzados. Afectando no solamente al equipo sino incluso el presupuesto asignado a este pudiendo ser reducido por no brindar los resultados esperados.

Recordemos que en la programación las cosas no son solamente agregar algunas líneas de instrucciones sino hacerlo de la mejor forma, escalable para que no afecte lo ya construido, y sostenible para evitar tener regresiones o pasos hacia atrás.
