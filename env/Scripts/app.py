from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+mysqlconnector://root@localhost/basefinace'
db = SQLAlchemy(app)

class Quote(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(10), nullable=False)
    date = db.Column(db.Date, nullable=False)
    open = db.Column(db.Float)
    high = db.Column(db.Float)
    low = db.Column(db.Float)
    close = db.Column(db.Float)
    volume = db.Column(db.Integer)
    adjclose = db.Column(db.Float)
    dividends = db.Column(db.Float)

    def __repr__(self):
        return f"<Quote(symbol={self.symbol}, date={self.date}, close={self.close})>"

@app.route('/quotes', methods=['POST'])
def add_quotes():
    data = request.get_json()
    for quote_data in data:
        quote = Quote(**quote_data)
        db.session.add(quote)
    db.session.commit()
    return 'Quotes added successfully!', 201

@app.route('/quotesget', methods=['GET'])
def get_quotes():
    symbol = request.args.get('symbol')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if not symbol:
        return jsonify({'error': 'You must provide a symbol parameter'}), 400

    quotes = Quote.query.filter_by(symbol=symbol)

    if start_date:
        quotes = quotes.filter(Quote.date >= start_date)

    if end_date:
        quotes = quotes.filter(Quote.date <= end_date)

    quotes = quotes.all()

    return jsonify([quote.__dict__ for quote in quotes]), 200

if __name__ == '__main__':
    app.run(debug=True)
