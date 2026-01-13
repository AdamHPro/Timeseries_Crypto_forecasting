import React, { useState } from "react";
import "./App.css";

const API_URL = process.env.REACT_APP_API_URL;

function App() {
  const [prediction, setPrediction] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  async function fetchPrediction() {
    setLoading(true);
    setError(null);

    try {
      const response = await fetch(`${API_URL}/predictions/latest`);
      console.log("Response:", response);

      if (!response.ok) {
        throw new Error("Impossible de récupérer la prédiction");
      }

      const data = await response.json();
      setPrediction(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="app">
      <div className="card">
        <h1>₿ Bitcoin Prediction</h1>
        <p className="subtitle">Prédiction de rendement BTC/USD</p>

        <button
          className="fetch-btn"
          onClick={fetchPrediction}
          disabled={loading}
        >
          {loading ? "Chargement..." : "Voir la dernière prédiction"}
        </button>

        {error && <div className="error">⚠️ {error}</div>}

        {prediction && !error && (
          <div className="prediction-result">
            <div className="prediction-value">
              <span className="label">Rendement prédit</span>
              <span
                className={`value ${
                  prediction.value >= 0 ? "positive" : "negative"
                }`}
              >
                {prediction.value >= 0 ? "+" : ""}
                {prediction.value.toFixed(4)}%
              </span>
            </div>
            <div className="prediction-meta">
              <span>Modèle: {prediction.model_version}</span>
              <span>
                Date: {new Date(prediction.created_at).toLocaleString("fr-FR")}
              </span>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default App;
