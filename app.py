import os, sys, traceback, io, base64
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
from pymongo import MongoClient
from pymongo.errors import ExecutionTimeout
from werkzeug.utils import secure_filename
from pyspark.sql import SparkSession  
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from bson.objectid import ObjectId
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
from tqdm import tqdm  # barra de progreso en consola
import time
import requests
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from flask_mail import Mail, Message
import random
from bson import ObjectId

# ───────── CONFIG ─────────
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, template_folder=os.path.join(BASE_DIR, "templates"), static_folder=os.path.join(BASE_DIR, "static"))
app.secret_key = os.environ.get('FLASK_SECRET', 'cambia_esto_en_produccion')

UPLOAD_FOLDER = os.path.join(app.static_folder, "posters")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
ALLOWED_EXT = {'png', 'jpg', 'jpeg'}

# Mongo
client = MongoClient("mongodb://localhost:27017")
db = client["Peliculas_database"]
users_col = db["usuarios"]
movies_col = db["peliculas"]
reviews_col = db["reseñas"]
follow_col = db["followers"]
watchlist_col = db["watchlist"]
links_col = db["links"]

app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 587
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USERNAME'] = 'rascaldoesnotdreamof@gmail.com'
app.config['MAIL_PASSWORD'] = 'lvqnrhzvutaxpbpz'
app.config['MAIL_DEFAULT_SENDER'] = 'rascaldoesnotdreamof.com'


mail = Mail(app)

# Spark: singleton controlado por variable de entorno (no se inicia por defecto).
spark_session = None
def get_spark_session_singleton():
    """
    Inicia Spark solo si ENABLE_SPARK=1 en el entorno.
    Esto evita que la app se quede colgada en entornos sin Spark.
    """
    global spark_session
    if spark_session is None:
        if os.environ.get('ENABLE_SPARK', '0') != '1':
            print("[INFO] Spark deshabilitado por defecto. Exporta ENABLE_SPARK=1 para activarlo.")
            return None
        # importamos Spark localmente
        try:
            from pyspark.sql import SparkSession
            spark_session = SparkSession.builder \
                .appName("PeliculasApp") \
                .config("spark.driver.memory", "4g") \
                .config("spark.executor.memory", "4g") \
                .config("spark.sql.shuffle.partitions", "50") \
                .getOrCreate()
            print("[INFO] Spark iniciado.")
        except Exception as e:
            print("[ERROR] No se pudo iniciar Spark:", e)
            spark_session = None
    return spark_session

# Helpers
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT

def image_to_base64_bytes(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", transparent=True)
    buf.seek(0)
    data = base64.b64encode(buf.read()).decode()
    plt.close(fig)
    return data

# -------------------------
# Autenticación y decoradores
# -------------------------
@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username'].strip()
        password = request.form['password']
        email = request.form.get('email','').strip()
        if users_col.find_one({'username': username}):
            flash('Usuario ya existe', 'error')
            return render_template('register.html')
        hashed = generate_password_hash(password)
        r = users_col.insert_one({'username': username, 'password': hashed, 'email': email, 'rol':'user', 'registered_at': datetime.utcnow()})
        session['user_id'] = str(r.inserted_id)
        session['username'] = username
        flash('Registro exitoso', 'success')
        return redirect(url_for('perfil'))
    return render_template('register.html')

@app.route('/login', methods=['GET','POST'])
def login():
    if request.method=='POST':
        username = request.form['username'].strip()
        password = request.form['password']
        user = users_col.find_one({'username': username})
        if not user:
            flash('Usuario no encontrado', 'error')
            return render_template('login.html')
        if not user.get('password'):
            flash('Cuenta con contraseña no válida', 'error')
            return render_template('login.html')
        valid = check_password_hash(user['password'], password)
        if valid:
            # Generar código de verificación
            code = str(random.randint(100000, 999999))
            session['temp_user_id'] = str(user['_id'])
            session['temp_username'] = user['username']
            session['2fa_code'] = code
            session['2fa_expire'] = time.time() + 300  # 5 minutos

            # Enviar correo
            try:
                msg = Message('Código de verificación - Ingreso seguro',
                            recipients=[user['email']])
                msg.body = f"Tu código de verificación es: {code}\n\nExpira en 5 minutos."
                mail.send(msg)
                flash('Se envió un código de verificación a tu correo.', 'info')
                return redirect(url_for('verify_2fa'))
            except Exception as e:
                print("[ERROR EMAIL]", e)
                flash('Error al enviar el código de verificación.', 'error')
                return render_template('login.html')
        else:
            flash('Credenciales inválidas', 'error')
    return render_template('login.html')

@app.route('/verify_2fa', methods=['GET', 'POST'])
def verify_2fa():
    if request.method == 'POST':
        code = request.form.get('code', '').strip()
        real_code = session.get('2fa_code')
        expire = session.get('2fa_expire', 0)
        if not real_code:
            flash('No hay código pendiente. Inicia sesión nuevamente.', 'error')
            return redirect(url_for('login'))
        if time.time() > expire:
            flash('El código ha expirado.', 'error')
            session.pop('2fa_code', None)
            return redirect(url_for('login'))
        if code == real_code:
            # Activar sesión definitiva
            user = users_col.find_one({'_id': ObjectId(session['temp_user_id'])})
            print("[DEBUG] Usuario logueado:", user)  # 🧩 Muestra qué rol tiene realmente
            session['user_id'] = str(user['_id'])
            session['username'] = user['username']
            session['rol'] = user.get('rol', 'user')
            print("[DEBUG] Rol cargado en sesión:", session['rol'])  # 🧩 Confirma qué valor tiene
            session.pop('2fa_code', None)
            session.pop('2fa_expire', None)
            session.pop('temp_user_id', None)
            session.pop('temp_username', None)
            flash('Verificación exitosa. Sesión iniciada.', 'success')
            return redirect(url_for('index'))
        else:
            flash('Código incorrecto.', 'error')
    return render_template('verify_2fa.html')

@app.route('/logout')
def logout():
    session.clear()
    flash('Sesión cerrada', 'success')
    return redirect(url_for('index'))

def require_login(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if 'user_id' not in session:
            flash('Debes iniciar sesión', 'error')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return wrapper

def require_admin(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if session.get('rol') != 'admin':
            flash('Acceso denegado (solo admin)', 'error')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return wrapper

# -------------------------
# CRUD Películas (admin)
# -------------------------
@app.route('/admin/movie/add', methods=['GET','POST'])
@require_login
@require_admin
def admin_add_movie():
    TRAILER_FOLDER = os.path.join(app.static_folder, "trailers")
    os.makedirs(TRAILER_FOLDER, exist_ok=True)
    ALLOWED_VIDEO_EXT = {'mp4', 'webm', 'ogg'}
    if request.method == 'POST':
        try:
            title = request.form['title'].strip()
            year = request.form.get('year') or None
            genres = [g.strip() for g in request.form.get('genres', '').split('|') if g.strip()]
            director = request.form.get('director', '').strip()
            cast = [c.strip() for c in request.form.get('cast', '').split('|') if c.strip()]

            last_movie = movies_col.find_one(sort=[("movieId", -1)])
            movieId = (last_movie['movieId'] + 1) if last_movie else 1

            poster_path = request.form.get('poster_path', '').strip() or None
            trailer_url = request.form.get('trailer_url', '').strip() or None
            trailer_file = request.files.get('trailer_file')

            trailer_path = None
            if trailer_file and trailer_file.filename != "":
                ext = trailer_file.filename.rsplit('.', 1)[1].lower()
                if ext in ALLOWED_VIDEO_EXT:
                    filename = secure_filename(f"{movieId}_{trailer_file.filename}")
                    trailer_path = f"trailers/{filename}"
                    trailer_file.save(os.path.join(TRAILER_FOLDER, filename))
                else:
                    flash('Formato de video no permitido', 'error')
                    return redirect(url_for('admin_add_movie'))

            # Guardar
            movies_col.insert_one({
                'movieId': movieId,
                'title': title,
                'year': year,
                'genres': genres,
                'director': director,
                'cast': cast,
                'poster_path': poster_path,
                'trailer_url': trailer_url,
                'trailer_path': trailer_path
            })

            flash(f'Película "{title}" agregada con ID {movieId}', 'success')
            return redirect(url_for('index'))
        except Exception as e:
            traceback.print_exc()
            flash('Error al agregar película', 'error')
    return render_template('admin_add_movie.html')

@app.route('/admin/movie/edit/<int:movieId>', methods=['GET','POST'])
@require_login
@require_admin
def admin_edit_movie(movieId):
    TRAILER_FOLDER = os.path.join(app.static_folder, "trailers")
    os.makedirs(TRAILER_FOLDER, exist_ok=True)
    ALLOWED_VIDEO_EXT = {'mp4', 'webm', 'ogg'}
    movie = movies_col.find_one({"movieId": movieId})
    if not movie:
        flash("Película no encontrada", "error")
        return redirect(url_for("index"))

    if request.method == "POST":
        title = request.form['title'].strip()
        year = request.form.get('year') or None
        genres = [g.strip() for g in request.form.get('genres', '').split('|') if g.strip()]
        director = request.form.get('director', '').strip()
        cast = [c.strip() for c in request.form.get('cast', '').split('|') if c.strip()]
        poster_path = None
         # Si sube archivo
        if 'poster_file' in request.files:
            file = request.files['poster_file']
            if file and file.filename != '':
                filename = secure_filename(file.filename)
                file.save(os.path.join(app.static_folder, 'posters', filename))
                poster_path = f'posters/{filename}'

        # Si no sube archivo pero puso link
        if not poster_path and request.form.get('poster_url'):
            poster_path = request.form['poster_url']

        trailer_url = request.form.get('trailer_url', '').strip() or None
        trailer_file = request.files.get('trailer_file')
        delete_trailer = 'delete_trailer' in request.form

        trailer_path = movie.get('trailer_path')

        # Eliminar trailer anterior si se marca la casilla
        if delete_trailer and trailer_path:
            try:
                os.remove(os.path.join(app.static_folder, trailer_path))
            except FileNotFoundError:
                pass
            trailer_path = None
            trailer_url = None

        # Subir nuevo archivo
        if trailer_file and trailer_file.filename != "":
            ext = trailer_file.filename.rsplit('.', 1)[1].lower()
            if ext in ALLOWED_VIDEO_EXT:
                filename = secure_filename(f"{movieId}_{trailer_file.filename}")
                trailer_path = f"trailers/{filename}"
                trailer_file.save(os.path.join(TRAILER_FOLDER, filename))
                trailer_url = None  # prioriza el archivo local
            else:
                flash("Formato de video no permitido", "error")
                return redirect(url_for("admin_edit_movie", movie_id=movieId))

        # Actualizar datos
        movies_col.update_one(
            {"movieId": movieId},
            {"$set": {
                "title": title,
                "year": year,
                "genres": genres,
                "director": director,
                "cast": cast,
                "poster_path": poster_path,
                "trailer_url": trailer_url,
                "trailer_path": trailer_path
            }}
        )

        flash("Película actualizada correctamente", "success")
        return redirect(url_for("index"))

    return render_template("admin_edit_movie.html", movie=movie)
# -------------------------
# Buscador / Index
# -------------------------
class MovieSearch:
    def __init__(self, mongo_collection, batch_size=1000):
        self.collection = mongo_collection
        self.batch_size = batch_size

    def _mongo_cursor_batches(self, filter_q, projection=None):
        cursor = self.collection.find(filter_q, projection or {})
        cursor.batch_size(self.batch_size)
        for doc in cursor:
            yield doc

    def search(self, qtext='', tipo='movies', limit=100):
        q = qtext.strip()
        results = []

        if tipo == 'users':
            cursor = users_col.find({'username': {'$regex': q, '$options': 'i'}}).limit(limit)
            for u in cursor:
                results.append({'username': u['username'], 'id': str(u['_id'])})
            return results

        if tipo == 'movies':
            projection = {'_id':0}
            idx = 0
            for doc in self._mongo_cursor_batches({'title': {'$regex': q, '$options': 'i'}} if q else {}, projection):
                results.append(doc)
                idx += 1
                if idx >= limit:
                    break
            return results

        if tipo == 'directors':
            cursor = movies_col.find({'director': {'$regex': q, '$options': 'i'}}, {'director':1, '_id':0}).limit(limit)
            directors_set = set()
            for doc in cursor:
                if doc.get('director'):
                    directors_set.add(doc['director'])
            return [{'director': d} for d in directors_set]

        if tipo == 'cast':
            cursor = movies_col.find({'cast': {'$regex': q, '$options': 'i'}}, {'cast':1, '_id':0}).limit(limit)
            cast_set = set()
            for doc in cursor:
                if doc.get('cast'):
                    cast_set.update([c for c in doc['cast'] if q.lower() in c.lower()])
            return [{'cast': c} for c in cast_set]

        if tipo == 'genres':
            cursor = movies_col.find({'genres': {'$regex': q, '$options': 'i'}}, {'genres':1, '_id':0}).limit(limit)
            genres_set = set()
            for doc in cursor:
                if doc.get('genres'):
                    # support list or string
                    if isinstance(doc['genres'], list):
                        candidates = [g for g in doc['genres'] if q.lower() in g.lower()]
                    else:
                        candidates = [g for g in str(doc['genres']).split('|') if q.lower() in g.lower()]
                    genres_set.update(candidates)
            return [{'genre': g} for g in genres_set]

        return []

movie_searcher = MovieSearch(movies_col, batch_size=500)

@app.route('/', methods=['GET'])
def index():
    q = request.args.get('busqueda', '').strip()
    tipo_busqueda = request.args.get('tipo_busqueda', 'movies')  # filtro por tipo

    peliculas, directores, cast, generos, usuarios = [], [], [], [], []
    popular_from_followed = []

    if q:
        # Buscar SOLO según el tipo elegido
        if tipo_busqueda == 'movies':
            peliculas = movie_searcher.search(q, tipo='movies', limit=50)
        elif tipo_busqueda == 'directors':
            directores = movie_searcher.search(q, tipo='directors', limit=50)
        elif tipo_busqueda == 'cast':
            cast = movie_searcher.search(q, tipo='cast', limit=50)
        elif tipo_busqueda == 'genres':
            generos = movie_searcher.search(q, tipo='genres', limit=50)
        elif tipo_busqueda == 'users':
            usuarios = movie_searcher.search(q, tipo='users', limit=50)
    else:
        # TOP películas por rating promedio
        pipeline = [
            {'$lookup': {
                'from': 'reseñas',
                'localField': 'movieId',
                'foreignField': 'movieId',
                'as': 'reviews'
            }},
            {'$addFields': {'rating_avg': {'$avg': '$reviews.rating'}}},
            {'$sort': {'rating_avg': -1}},
            {'$limit': 10}
        ]
        try:
            peliculas = list(movies_col.aggregate(pipeline, allowDiskUse=True, maxTimeMS=2000))
            if not peliculas:
                peliculas = list(movies_col.find({}, {'_id': 0}).sort('movieId', -1).limit(10))
        except Exception as e:
            print("[WARN] Error al obtener películas:", e)
            peliculas = list(movies_col.find({}, {'_id': 0}).limit(10))

        # Popular entre seguidos
        try:
            if 'user_id' in session:
                me = users_col.find_one({'_id': ObjectId(session['user_id'])})
                if me:
                    followed = list(follow_col.find({'follower': me['_id']}))
                    followed_ids = [f['followed'] for f in followed]
                    if followed_ids:
                        pipeline_f = [
                            {'$match': {'userId': {'$in': followed_ids}}},
                            {'$group': {'_id': '$movieId', 'count': {'$sum': 1}}},
                            {'$sort': {'count': -1}},
                            {'$limit': 10}
                        ]
                        top_followed = list(reviews_col.aggregate(pipeline_f, maxTimeMS=2000))
                        movie_ids = [t['_id'] for t in top_followed if t.get('_id') is not None]
                        if movie_ids:
                            popular_from_followed = list(movies_col.find({'movieId': {'$in': movie_ids}}))
                            id_to_movie = {m['movieId']: m for m in popular_from_followed}
                            popular_from_followed = [id_to_movie[mid] for mid in movie_ids if mid in id_to_movie]
        except Exception as e:
            print("[WARN] Error al generar 'popular_from_followed':", e)

    return render_template('index.html',
                           busqueda=q,
                           tipo_busqueda=tipo_busqueda,
                           peliculas=peliculas,
                           directores=directores,
                           cast=cast,
                           generos=generos,
                           usuarios=usuarios,
                           popular_from_followed=popular_from_followed)

from bson.objectid import ObjectId
@app.route('/user/<username>')
def user_profile(username):
    user = users_col.find_one({"username": username})
    if not user:
        flash("Usuario no encontrado", "error")
        return redirect(url_for("index"))
    return render_template("user_profile.html", user=user)

@app.route('/movie/<int:movieId>')
def movie_detail(movieId):
    movie = movies_col.find_one({'movieId': movieId})
    if not movie:
        flash('Película no encontrada', 'error')
        return redirect(url_for('index'))

    # Normalizar géneros
    genres = movie.get('genres', [])
    if isinstance(genres, str):
        genres = [g.strip() for g in genres.split('|') if g.strip()]
    movie['genres'] = genres

    # Obtener reseñas locales con usuario y fecha
    local_reviews = list(
        reviews_col.find({'movieId': movieId}).sort('created_at', -1).limit(200)
    )

    for r in local_reviews:
        uid = r.get("userId")
        if not isinstance(uid, ObjectId):
            try:
                uid = ObjectId(uid)
            except:
                uid = None
        user = users_col.find_one({"_id": uid}) if uid else None
        r["username"] = user["username"] if user else "Desconocido"

        # Convertir fecha a datetime si viene como string
        if isinstance(r.get("created_at"), str):
            try:
                r["created_at"] = datetime.fromisoformat(r["created_at"])
            except:
                r["created_at"] = None

    # Calificación promedio local
    local_ratings = [r['rating'] for r in local_reviews if r.get('rating') is not None]
    avg_local = round(sum(local_ratings) / len(local_ratings), 2) if local_ratings else None

    # Gráfico de distribución de calificaciones locales
    b64_graph = None
    if local_ratings:
        sns.set_theme(style="darkgrid")
        fig, ax = plt.subplots(figsize=(6, 4))
        sns.histplot(local_ratings, bins=5, kde=False, ax=ax)
        ax.set_title("Distribución de Calificaciones Locales")
        ax.set_xlabel("Calificación")
        ax.set_ylabel("Cantidad")
        b64_graph = image_to_base64_bytes(fig)

    # Usuario actual
    user_id = session.get("user_id")
    is_admin = False
    if user_id:
        try:
            current_user = users_col.find_one({"_id": ObjectId(user_id)})
            is_admin = current_user.get("rol") == "admin"
        except:
            pass

    return render_template(
        'movie_detail.html',
        movie=movie,
        reviews=local_reviews,
        avg_rating=avg_local,
        ratings_graph=b64_graph,
        is_admin=is_admin
    )

@app.route('/movie/<int:movieId>/edit', methods=['GET'])
@require_login
@require_admin
def edit_movie_form(movieId):
    movie = movies_col.find_one({'movieId': movieId})
    if not movie:
        flash('Película no encontrada', 'error')
        return redirect(url_for('index'))
    return render_template('admin_edit_movie.html', movie=movie)

from flask import current_app, flash, redirect, url_for, request
from bson.objectid import ObjectId
@app.route('/movie/<int:movieId>/edit', methods=['POST'])
@require_login
@require_admin
def edit_movie(movieId):
    movie = movies_col.find_one({"movieId": movieId})
    if not movie:
        flash("Película no encontrada", "error")
        return redirect(url_for("index"))

    if request.method == "POST":
        title = request.form.get("title")
        year = int(request.form.get("year")) if request.form.get("year") else None
        director = request.form.get("director")
        genres = [g.strip() for g in request.form.get("genres", "").split("|")]
        cast = [c.strip() for c in request.form.get("cast", "").split("|")]

        poster = request.files.get("poster")
        if poster and poster.filename != "":
            poster_filename = secure_filename(f"{movieId}_{poster.filename}")
            poster_path = f"posters/{poster_filename}"
            poster.save(os.path.join(app.static_folder, poster_path))
        else:
            poster_path = movie.get("poster_path")  # Mantener el existente

        movies_col.update_one({"movieId": movieId}, {
            "$set": {
                "title": title,
                "year": year,
                "director": director,
                "genres": genres,
                "cast": cast,
                "poster_path": poster_path
            }
        })

        flash("Película actualizada correctamente.", "success")
        return redirect(url_for("movie_detail", movieId=movieId))

    return render_template("edit_movie.html", movie=movie)
# -------------------------
# Reviews / watchlist / follow
# -------------------------
@app.route('/movie/<int:movieId>/rate', methods=['POST'])
def rate_movie(movieId):
    if 'user_id' not in session:
        flash('Debes iniciar sesión para dejar una reseña.', 'error')
        return redirect(url_for('login'))

    user_id = ObjectId(session['user_id'])
    rating = float(request.form.get('rating', 0))
    review_text = request.form.get('review_text', '').strip()

    # Guardar la reseña con fecha actual
    review_data = {
        'movieId': movieId,
        'userId': user_id,
        'rating': rating,
        'text': review_text,
        'created_at': datetime.utcnow()
    }

    reviews_col.insert_one(review_data)
    flash('Tu reseña ha sido agregada correctamente.', 'success')
    return redirect(url_for('movie_detail', movieId=movieId))

@app.route('/watchlist/add/<int:movieId>')
@require_login
def add_watchlist(movieId):
    watchlist_col.update_one({'userId': ObjectId(session['user_id']), 'movieId': movieId}, {'$set':{'userId': ObjectId(session['user_id']), 'movieId': movieId}}, upsert=True)
    flash('Añadido a watchlist','success')
    return redirect(url_for('perfil'))

@app.route('/watchlist/remove/<int:movieId>')
@require_login
def remove_watchlist(movieId):
    watchlist_col.delete_one({'userId': ObjectId(session['user_id']), 'movieId': movieId})
    flash('Eliminado de watchlist','success')
    return redirect(url_for('perfil'))


@app.route('/follow/<username>')
@require_login
def follow_user(username):
    target = users_col.find_one({'username': username})
    if not target:
        flash('Usuario no existe','error')
        return redirect(url_for('index'))
    follow_col.update_one({'follower': ObjectId(session['user_id']), 'followed': target['_id']}, {'$set': {'follower': ObjectId(session['user_id']), 'followed': target['_id']}}, upsert=True)
    flash('Ahora sigues a '+username,'success')
    return redirect(url_for('user_profile', username=username))

@app.route('/unfollow/<username>')
@require_login
def unfollow_user(username):
    target = users_col.find_one({'username': username})
    if target:
        follow_col.delete_one({'follower': ObjectId(session['user_id']), 'followed': target['_id']})
    flash('Dejaste de seguir a '+username,'success')
    return redirect(url_for('user_profile', username=username))

@app.route('/perfil')
@require_login
def perfil():
    user = users_col.find_one({'_id': ObjectId(session['user_id'])})

    # Películas favoritas
    fav_movies = []
    if "fav_movies" in user:
        for title in user["fav_movies"]:
            movie = movies_col.find_one({'title': title})
            fav_movies.append(movie if movie else {"title": title, "poster_path": None, "year": None, "movieId": None})

    # Reseñas
    reseñas = []
    for r in reviews_col.find({'userId': user['_id']}).sort('created_at', -1).limit(100):
        movie = movies_col.find_one({'movieId': r['movieId']})
        r['movie'] = movie if movie else {"title": "Desconocida", "poster_path": None}
        reseñas.append(r)

    # Watchlist
    watchlist = []
    for w in watchlist_col.find({'userId': user['_id']}).limit(100):
        movie = movies_col.find_one({'movieId': w['movieId']})
        w['movie'] = movie if movie else {"title": "Desconocida", "poster_path": None}
        watchlist.append(w)

    following = list(follow_col.find({'follower': user['_id']}))
    followers = list(follow_col.find({'followed': user['_id']}))

    return render_template(
        'perfil.html',
        user=user,
        fav_movies=fav_movies,
        reseñas=reseñas,
        watchlist=watchlist,
        following=following,
        followers=followers
    )

@app.route('/perfil/edit', methods=['GET','POST'])
@require_login
def edit_profile():
    user = users_col.find_one({'_id': ObjectId(session['user_id'])})
    if request.method == 'POST':
        update_data = {}
        if request.form.get('fav_movies'):
            update_data['fav_movies'] = [m.strip() for m in request.form.get('fav_movies').split('|')]
        if request.files.get('profile_pic'):
            pic = request.files['profile_pic']
            if allowed_file(pic.filename):
                filename = secure_filename(f"profile_{user['_id']}_{pic.filename}")
                pic.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
                update_data['profile_pic'] = f'posters/{filename}'
        users_col.update_one({'_id': user['_id']}, {'$set': update_data})
        flash('Perfil actualizado', 'success')
        return redirect(url_for('perfil'))
    return render_template('edit_profile.html', user=user)

# -------------------------
# ADMIN: list de análisis y ejecución
# -------------------------
@app.route('/admin/analisis')
@require_login
@require_admin
def analisis_index():
    analyses = [
    {'id':'top_genres','title':'Top géneros'},
    {'id':'movies_per_year','title':'Películas por año'},
    {'id':'top_directors','title':'Top directores'},
    {'id':'top_actors','title':'Top actores'},
    {'id':'ratings_distribution','title':'Distribución de calificaciones'},
    {'id':'linear_regression','title':'Regresión Lineal'},
    {'id':'polynomial_regression','title':'Regresión Polinomial'} 
]
    return render_template('analisis.html', analyses=analyses)

@app.route('/admin/analisis/run/<analysis_id>')
@require_login
@require_admin
def run_admin_analysis(analysis_id):
    q = request.args.get('busqueda', '').strip()
    use_spark = request.args.get('spark', '0') == '1'

    # --- Configuración estilo gráficos ---
    sns.set_theme(style="darkgrid")
    plt.rcParams.update({
        "axes.facecolor": "#2E2E2E",
        "figure.facecolor": "#2E2E2E",
        "axes.edgecolor": "white",
        "axes.labelcolor": "white",
        "xtick.color": "white",
        "ytick.color": "white",
        "text.color": "white",
        "grid.color": "#555555",
        "grid.linestyle": "--",
    })
    sns.set_palette("bright")
    def run_with_progress_iterable(iterable, desc="Procesando"):
        print(f"[INFO] {desc}...")
        return iterable

    def df_from_pipeline(pipeline, collection=movies_col, desc="Pipeline"):
        rows = list(run_with_progress_iterable(
            collection.aggregate([p for p in pipeline], allowDiskUse=True),
            desc=desc
        ))
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    spark = None
    if use_spark:
        spark = get_spark_session_singleton()
        if spark is None:
            flash('Spark no disponible. Ejecutando con Mongo.', 'warning')
            use_spark = False

    # -----------------------------
    # TOP GÉNEROS
    # -----------------------------
    if analysis_id == 'top_genres':
        if use_spark and spark:
            sample_docs = list(run_with_progress_iterable(
                movies_col.find({}, {'movieId': 1, 'genres': 1}).limit(5000),
                desc="Cargando muestra Spark"
            ))
            if not sample_docs:
                flash('No hay datos para Spark.', 'error')
                return redirect(url_for('analisis_index'))

            pdf = pd.DataFrame(sample_docs)
            sdf = spark.createDataFrame(pdf)
            from pyspark.sql.functions import split, explode, col, trim
            sdf2 = sdf.withColumn('genres', explode(split(col('genres'), '\\|')))
            sdf2 = sdf2.withColumn('genres', trim(col('genres')))
            grouped = sdf2.groupBy('genres').count().orderBy('count', ascending=False).limit(50)

            rows = [{'genre': r['genres'], 'count': int(r['count'])} for r in grouped.collect()]
            df = pd.DataFrame(rows)
        else:
            pipeline = []
            if q:
                pipeline.append({'$match': {'genres': {'$regex': q, '$options': 'i'}}})

            pipeline += [
                {'$project': {
                    'genres': {
                        '$cond': [
                            {'$eq': [{'$type': "$genres"}, "string"]},
                            {'$split': ["$genres", "|"]},
                            "$genres"
                        ]
                    }
                }},
                {'$unwind': '$genres'},
                {'$group': {'_id': '$genres', 'count': {'$sum': 1}}},
                {'$sort': {'count': -1}}, {'$limit': 50},
                {'$project': {'genre': '$_id', 'count': 1, '_id': 0}}
            ]

            df = df_from_pipeline(pipeline, desc="Procesando géneros")

        if not df.empty:
            fig, ax = plt.subplots(figsize=(10, 5))
            sns.barplot(x='genre', y='count', data=df, ax=ax)
            ax.set_title("Top Géneros")
            plt.xticks(rotation=45, ha='right')
            return render_template('analysis_result.html',
                                title='Top géneros',
                                summary=f"Top {len(df)} géneros",
                                img_b64=image_to_base64_bytes(fig),
                                tabla=df.to_dict(orient='records'))

    # -----------------------------
    # PELÍCULAS POR AÑO
    # -----------------------------
    if analysis_id == 'movies_per_year':
        pipeline = []
        if q:
            pipeline.append({'$match': {'title': {'$regex': q, '$options': 'i'}}})
        pipeline += [
            {'$group': {'_id': '$year', 'count': {'$sum': 1}}},
            {'$sort': {'_id': 1}}
        ]
        df = df_from_pipeline(pipeline, desc="Agrupando por año")
        if not df.empty:
            fig, ax = plt.subplots(figsize=(10, 5))
            sns.lineplot(data=df, x='_id', y='count', marker='o', ax=ax)
            ax.set_title("Películas por Año")
            return render_template('analysis_result.html',
                                   title='Películas por Año',
                                   summary="Conteo anual",
                                   img_b64=image_to_base64_bytes(fig),
                                   tabla=df.to_dict(orient='records'))

    # -----------------------------
    # TOP DIRECTORES
    # -----------------------------
    if analysis_id == 'top_directors':
        pipeline = []
        if q:
            pipeline.append({'$match': {'director': {'$regex': q, '$options': 'i'}}})
        pipeline += [
            {'$group': {'_id': '$director', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}}, {'$limit': 20}
        ]
        df = df_from_pipeline(pipeline, desc="Procesando directores")
        if not df.empty:
            fig, ax = plt.subplots(figsize=(10, 5))
            sns.barplot(x='_id', y='count', data=df, ax=ax)
            ax.set_title("Top Directores")
            plt.xticks(rotation=45, ha='right')
            return render_template('analysis_result.html',
                                   title='Top Directores',
                                   summary="Directores con más películas",
                                   img_b64=image_to_base64_bytes(fig),
                                   tabla=df.to_dict(orient='records'))

    # -----------------------------
    # TOP ACTORES
    # -----------------------------
    if analysis_id == 'top_actors':
        pipeline = []
        if q:
            pipeline.append({'$match': {'cast': {'$regex': q, '$options': 'i'}}})
        pipeline += [
            {'$unwind': '$cast'},
            {'$group': {'_id': '$cast', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}}, {'$limit': 20}
        ]
        df = df_from_pipeline(pipeline, desc="Procesando actores")
        if not df.empty:
            fig, ax = plt.subplots(figsize=(10, 5))
            sns.barplot(x='_id', y='count', data=df, ax=ax)
            ax.set_title("Top Actores")
            plt.xticks(rotation=45, ha='right')
            return render_template('analysis_result.html',
                                   title='Top Actores',
                                   summary="Actores con más películas",
                                   img_b64=image_to_base64_bytes(fig),
                                   tabla=df.to_dict(orient='records'))

    # -----------------------------
    # DISTRIBUCIÓN DE CALIFICACIONES
    # -----------------------------
    if analysis_id == 'ratings_distribution':
        pipeline = []
        if q:
            pipeline.append({'$match': {'text': {'$regex': q, '$options': 'i'}}})
        pipeline += [
            {'$group': {'_id': '$rating', 'count': {'$sum': 1}}},
            {'$sort': {'_id': 1}}
        ]
        rows = list(run_with_progress_iterable(reviews_col.aggregate(pipeline, allowDiskUse=True),
                                               desc="Procesando calificaciones"))
        df = pd.DataFrame(rows) if rows else pd.DataFrame()
        if not df.empty:
            fig, ax = plt.subplots(figsize=(8, 5))
            sns.barplot(x='_id', y='count', data=df, ax=ax)
            ax.set_title("Distribución de Calificaciones")
            ax.set_xlabel("Calificación")
            ax.set_ylabel("Cantidad")
            return render_template('analysis_result.html',
                                   title='Distribución de Calificaciones',
                                   summary="Histograma de calificaciones",
                                   img_b64=image_to_base64_bytes(fig),
                                   tabla=df.to_dict(orient='records'))
    # -----------------------------
    # REGRESIÓN LINEAL Y POLINOMIAL
    # -----------------------------
    if analysis_id == 'regresion':
        import re
        from sklearn.linear_model import LinearRegression
        from sklearn.preprocessing import PolynomialFeatures
        from sklearn.metrics import r2_score
        import numpy as np

        # --- Extraer películas ---
        pipeline_movies = []
        if q:
            pipeline_movies.append({'$match': {'title': {'$regex': q, '$options': 'i'}}})
        pipeline_movies += [
            {'$project': {'movieId': 1, 'title': 1, 'year': 1}}
        ]
        movies_rows = list(movies_col.aggregate(pipeline_movies, allowDiskUse=True))
        if not movies_rows:
            flash('No hay películas para análisis.', 'error')
            return redirect(url_for('analisis_index'))

        movies_df = pd.DataFrame(movies_rows)

        # --- Extraer año de columna o título ---
        def extract_year(row):
            if pd.notna(row.get('year')):
                try:
                    return int(row['year'])
                except:
                    pass
            match = re.search(r'\b(19\d{2}|20\d{2})\b', row['title'])
            return int(match.group(0)) if match else None

        movies_df['year'] = movies_df.apply(extract_year, axis=1)
        movies_df.dropna(subset=['year'], inplace=True)
        movies_df['year'] = movies_df['year'].astype(int)

        # Convertir movieId a int
        movies_df['movieId'] = movies_df['movieId'].astype(int)

        # --- Extraer ratings promedio desde reseñas ---
        pipeline_ratings = [
            {'$group': {'_id': '$movieId', 'avg_rating': {'$avg': '$rating'}}}
        ]
        ratings_rows = list(reviews_col.aggregate(pipeline_ratings, allowDiskUse=True))
        ratings_df = pd.DataFrame(ratings_rows)
        if ratings_df.empty:
            flash('No hay ratings disponibles para regresión.', 'error')
            return redirect(url_for('analisis_index'))

        ratings_df.rename(columns={'_id': 'movieId'}, inplace=True)
        ratings_df['movieId'] = ratings_df['movieId'].astype(int)

        # --- Merge películas con ratings ---
        df = pd.merge(movies_df, ratings_df, on='movieId', how='inner')
        df.dropna(subset=['avg_rating'], inplace=True)

        # Revisar cuántos datos tenemos
        if len(df) < 5:
            flash(f'No hay suficientes datos para regresión. Datos disponibles: {len(df)}', 'error')
            return redirect(url_for('analisis_index'))

        X = df[['year']].values
        y = df['avg_rating'].values

        # --- Regresión Lineal ---
        lin_reg = LinearRegression()
        lin_reg.fit(X, y)
        y_pred_lin = lin_reg.predict(X)
        r2_lin = r2_score(y, y_pred_lin)

        # --- Regresión Polinomial grado 2 ---
        poly = PolynomialFeatures(degree=2)
        X_poly = poly.fit_transform(X)
        poly_reg = LinearRegression()
        poly_reg.fit(X_poly, y)
        y_pred_poly = poly_reg.predict(X_poly)
        r2_poly = r2_score(y, y_pred_poly)

        # --- Gráfico ---
        fig, ax = plt.subplots(figsize=(10,6))
        ax.scatter(X, y, color='lightgreen', label='Datos reales')
        ax.plot(X, y_pred_lin, color='cyan', linewidth=2, label=f'Lineal (R²={r2_lin:.2f})')
        ax.plot(X, y_pred_poly, color='magenta', linewidth=2, label=f'Polinomial (R²={r2_poly:.2f})')
        ax.set_xlabel('Año')
        ax.set_ylabel('Rating Promedio')
        ax.set_title('Regresión Lineal y Polinomial de Ratings por Año')
        ax.legend()
        plt.xticks(rotation=45)
        plt.grid(True)

        return render_template('analysis_result.html',
                            title='Regresión Ratings vs Año',
                            summary=f'Regresión Lineal y Polinomial sobre {len(df)} películas',
                            img_b64=image_to_base64_bytes(fig),
                            tabla=df.to_dict(orient='records'))

    # -----------------------------
    # DEFAULT
    # -----------------------------
    return render_template('analysis_result.html',
                           title='Análisis',
                           summary='Sin datos',
                           img_b64=None,
                           tabla=[])

# -------------------------
# API búsqueda
# -------------------------|
@app.route('/api/search')
def api_search():
    q = request.args.get('q','').strip()
    tipo = request.args.get('tipo','movies')
    results = movie_searcher.search(q, tipo=tipo, limit=100)
    return jsonify(results)

# Run
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
