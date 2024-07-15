import os
from flask import Flask, jsonify
from flask_cors import CORS
from flask_jwt_extended import JWTManager, jwt_required, get_jwt_identity
from flask_sqlalchemy import SQLAlchemy
from dotenv import load_dotenv

# Initialize Flask app
app = Flask(__name__)

# Load environment variables
load_dotenv()

# Configure app settings
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['JWT_SECRET_KEY'] = os.getenv('JWT_SECRET_KEY')

# Initialize extensions
db = SQLAlchemy(app)
CORS(app)
jwt = JWTManager(app)

# Models
class MainArtist(db.Model):
    __tablename__ = 'MAIN_ARTIST'
    main_artist_id = db.Column(db.String(50), primary_key=True)
    main_artist_name = db.Column(db.String(100), nullable=False)
    main_artist_popularity = db.Column(db.Integer, nullable=False)
    main_artist_genre = db.Column(db.String(100), nullable=False)
    main_artist_image_url = db.Column(db.Text)
    
    # Relationship to the Track table
    tracks = db.relationship('Track', backref='main_artist', lazy=True)

class Track(db.Model):
    __tablename__ = 'TRACK'
    track_id = db.Column(db.String(50), primary_key=True)
    main_artist_id = db.Column(db.String(50), db.ForeignKey('MAIN_ARTIST.main_artist_id'), nullable=False)
    released_date = db.Column(db.Date, nullable=False)
    loudness = db.Column(db.Numeric(8, 4))
    tempo = db.Column(db.Numeric(8, 4))
    tempo_confidence = db.Column(db.Numeric(8, 4))
    time_signature = db.Column(db.Numeric(8, 4))
    time_signature_confidence = db.Column(db.Numeric(8, 4))
    track_name = db.Column(db.String(100), nullable=False)
    track_popularity = db.Column(db.Integer, nullable=False)
    other_artists = db.Column(db.String(100))
    track_image_url = db.Column(db.Text)
    lyrics = db.Column(db.Text)


class Utilisateur(db.Model):
    __tablename__ = 'utilisateur'
    user_id = db.Column(db.BigInteger, primary_key=True)
    username = db.Column(db.String(255), nullable=False)
    email = db.Column(db.String(255), nullable=False, unique=True)
    password = db.Column(db.String(255), nullable=False)

    def set_password(self, password):
        self.password = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password, password)


# Routes
@app.route('/')
def index():
    return "Welcome to the Flask App"

@app.route('/auth/register/', methods=['POST'])
def register():
    data = request.get_json()
    if not data:
        return jsonify({"msg": "Missing JSON in request"}), 400

    username = data.get('username')
    email = data.get('email')
    password = data.get('password')

    if not username or not email or not password:
        return jsonify({"msg": "Missing username, email, or password"}), 400

    if Utilisateur.query.filter_by(email=email).first():
        return jsonify({"msg": "Email already registered"}), 400

    new_user = Utilisateur(username=username, email=email)
    new_user.set_password(password)
    db.session.add(new_user)
    db.session.commit()

    return jsonify({"msg": "User created successfully"}), 200

@app.route('/auth/login/', methods=['POST'])
def login():
    data = request.get_json()
    email = data.get('email')
    password = data.get('password')

    user = Utilisateur.query.filter_by(email=email).first()
    if user and user.check_password(password):
        access_token = create_access_token(identity=user.email)
        return jsonify(access_token=access_token, username=user.username, user_id=user.user_id), 200

    return jsonify({"msg": "Bad email or password"}), 401

@app.route('/test/', methods=['GET'])
def test():
    return "test"

@app.route('/artists/all', methods=['GET'])
@jwt_required()
def get_all_artists():
    artists = MainArtist.query.all()
    artist_list = [{
        "main_artist_id": artist.main_artist_id,
        "main_artist_name": artist.main_artist_name,
        "main_artist_popularity": artist.main_artist_popularity,
        "main_artist_genre": artist.main_artist_genre,
        "main_artist_image_url": artist.main_artist_image_url
    } for artist in artists]
    return jsonify(artist_list), 200

@app.route('/tracks/all', methods=['GET'])
@jwt_required()
def get_all_tracks():
    tracks = Track.query.all()
    track_list = [{
        "track_id": track.track_id,
        "main_artist_id": track.main_artist_id,
        "released_date": track.released_date,
        "loudness": track.loudness,
        "tempo": track.tempo,
        "tempo_confidence": track.tempo_confidence,
        "time_signature": track.time_signature,
        "time_signature_confidence": track.time_signature_confidence,
        "track_name": track.track_name,
        "track_popularity": track.track_popularity,
        "other_artists": track.other_artists,
        "track_image_url": track.track_image_url,
        "lyrics": track.lyrics
    } for track in tracks]
    return jsonify(track_list), 200

# Run the app
if __name__ == '__main__':
    app.run(debug=True)