import React, { useState } from 'react';
import P2PDashboard from './p2p';
import O2CDashboard from './o2c';
import './App.css';

const API = 'http://localhost:8000';

/* ─── SHARED LOGIN SCREEN ─────────────────────────────────────────────────── */
const LoginScreen = ({ onLogin }) => {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError]       = useState('');
  const [loading, setLoading]   = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError('');
    try {
      const res = await fetch(`${API}/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || 'Login failed');
      onLogin(data.username);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{
      position: 'fixed', inset: 0,
      background: '#F0F2F5',
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      fontFamily: "'Segoe UI', system-ui, sans-serif",
    }}>
      <div style={{
        background: '#1B3A2A',
        padding: '44px 40px',
        borderRadius: 10,
        width: 360,
        boxShadow: '0 12px 40px rgba(0,0,0,0.18)',
        textAlign: 'center',
      }}>
        <img
          src="/logo.png"
          alt="ajaLabs Logo"
          onError={e => { e.target.style.display = 'none'; }}
          style={{ height: 60, margin: '0 auto 20px', display: 'block', borderRadius: 8 }}
        />

        <h2 style={{ margin: '0 0 6px', color: '#e0dedc', fontSize: 20, fontWeight: 700 }}>
          Process Mining
        </h2>
        <p style={{ margin: '0 0 28px', fontSize: 13, color: '#9a9590' }}>
          Sign in to access your dashboard
        </p>

        {error && (
          <div style={{
            background: '#FDE7E9', color: '#A4262C',
            padding: '8px 12px', borderRadius: 4,
            fontSize: 12, marginBottom: 16, textAlign: 'left',
          }}>
            {error}
          </div>
        )}

        <form onSubmit={handleSubmit} style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
          <input
            type="text"
            placeholder="Username"
            required
            value={username}
            onChange={e => setUsername(e.target.value)}
            style={{
              padding: '10px 12px', borderRadius: 4,
              border: '1px solid rgba(255,255,255,0.15)',
              background: 'rgba(255,255,255,0.08)',
              color: '#fff', fontSize: 14, outline: 'none',
            }}
          />
          <input
            type="password"
            placeholder="Password"
            required
            value={password}
            onChange={e => setPassword(e.target.value)}
            style={{
              padding: '10px 12px', borderRadius: 4,
              border: '1px solid rgba(255,255,255,0.15)',
              background: 'rgba(255,255,255,0.08)',
              color: '#fff', fontSize: 14, outline: 'none',
            }}
          />
          <button
            type="submit"
            disabled={loading}
            style={{
              background: loading ? '#4a7a5e' : '#006B3C',
              color: '#fff', padding: '11px', border: 'none',
              borderRadius: 4, fontSize: 14, fontWeight: 700,
              cursor: loading ? 'not-allowed' : 'pointer',
              marginTop: 4,
              transition: 'background 0.2s',
            }}
          >
            {loading ? 'Signing in…' : 'Sign In'}
          </button>
        </form>
      </div>

      <div style={{
        position: 'absolute', bottom: 24, width: '100%',
        textAlign: 'center', fontSize: 12, color: '#605E5C',
      }}>
        ©2026{' '}
        <a href="https://ajalabs.ai" target="_blank" rel="noopener noreferrer"
          style={{ color: '#323130', textDecoration: 'none', fontWeight: 700 }}>
          ajaLabs.ai
        </a>{' '}
        All rights reserved ·{' '}
        <a href="#" style={{ color: '#0078D4', textDecoration: 'none' }}>Data Privacy</a>
      </div>
    </div>
  );
};

/* ─── MODULE SELECTOR ─────────────────────────────────────────────────────── */
const ModuleSelector = ({ currentUser, onSelect, onSignOut }) => (
  <div style={{
    minHeight: '100vh', background: '#F0F2F5',
    fontFamily: "'Segoe UI', system-ui, sans-serif",
    display: 'flex', flexDirection: 'column',
  }}>
    {/* Header */}
    <div style={{
      background: '#1B3A2A', padding: '14px 28px',
      display: 'flex', justifyContent: 'space-between', alignItems: 'center',
      boxShadow: '0 2px 8px rgba(0,0,0,0.2)',
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
        <img
          src="/logo.png"
          alt="ajaLabs Logo"
          onError={e => { e.target.style.display = 'none'; }}
          style={{ height: 36, borderRadius: 6 }}
        />
        <div>
          <div style={{ color: '#fff', fontWeight: 700, fontSize: 16 }}>Process Mining</div>
          <div style={{ color: 'rgba(255,255,255,0.5)', fontSize: 11 }}>ajaLabs Select a module</div>
        </div>
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
        <span style={{ fontSize: 13, color: 'rgba(255,255,255,0.7)' }}>
          Signed in as <strong style={{ color: '#fff' }}>{currentUser}</strong>
        </span>
        <button
          onClick={onSignOut}
          style={{
            background: 'rgba(209,52,56,0.85)', color: '#fff',
            border: 'none', padding: '6px 14px', borderRadius: 4,
            cursor: 'pointer', fontSize: 12, fontWeight: 700,
          }}
          onMouseOver={e => e.currentTarget.style.background = '#D13438'}
          onMouseOut={e => e.currentTarget.style.background = 'rgba(209,52,56,0.85)'}
        >
          Sign Out
        </button>
      </div>
    </div>

    {/* Cards */}
    <div style={{
      flex: 1, display: 'flex', flexDirection: 'column',
      alignItems: 'center', justifyContent: 'center',
      padding: '3rem 2rem',
    }}>
      <h1 style={{ fontSize: '1.9rem', color: '#1e293b', margin: '0 0 0.5rem', fontWeight: 700 }}>
        Select a Module
      </h1>
      <p style={{ color: '#64748b', fontSize: 14, margin: '0 0 3rem' }}>
        Choose the process you want to analyse
      </p>

      <div style={{ display: 'flex', gap: '2rem', flexWrap: 'wrap', justifyContent: 'center' }}>

        <div
          onClick={() => onSelect('p2p')}
          style={cardStyle}
          onMouseEnter={e => Object.assign(e.currentTarget.style, cardHover)}
          onMouseLeave={e => Object.assign(e.currentTarget.style, cardStyle)}
        >
          <div style={{ width: 48, height: 48, background: '#EBF5FF', borderRadius: 10, marginBottom: 16,
            display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none">
              <path d="M3 3h18v4H3zM3 10h18v4H3zM3 17h18v4H3z" fill="#0078D4" opacity="0.15"/>
              <rect x="3" y="3" width="18" height="4" rx="1" stroke="#0078D4" strokeWidth="1.5" fill="none"/>
              <rect x="3" y="10" width="18" height="4" rx="1" stroke="#0078D4" strokeWidth="1.5" fill="none"/>
              <rect x="3" y="17" width="18" height="4" rx="1" stroke="#0078D4" strokeWidth="1.5" fill="none"/>
            </svg>
          </div>
          <h2 style={{ fontSize: '1.05rem', margin: '0 0 8px', color: '#1e293b', fontWeight: 700 }}>
            Procure-to-Pay
          </h2>
          <p style={{ fontSize: 13, color: '#64748b', margin: '0 0 20px', lineHeight: 1.5 }}>
            Analyse purchasing, goods receipts, and invoice timelines.
          </p>
          <button style={{ ...btnStyle, background: '#0078D4' }}>Launch P2P</button>
        </div>

        <div
          onClick={() => onSelect('o2c')}
          style={cardStyle}
          onMouseEnter={e => Object.assign(e.currentTarget.style, cardHover)}
          onMouseLeave={e => Object.assign(e.currentTarget.style, cardStyle)}
        >
          <div style={{ width: 48, height: 48, background: '#EDFAF4', borderRadius: 10, marginBottom: 16,
            display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none">
              <circle cx="12" cy="12" r="9" fill="#006B3C" opacity="0.12"/>
              <path d="M8 12l3 3 5-5" stroke="#006B3C" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
            </svg>
          </div>
          <h2 style={{ fontSize: '1.05rem', margin: '0 0 8px', color: '#1e293b', fontWeight: 700 }}>
            Order-to-Cash
          </h2>
          <p style={{ fontSize: 13, color: '#64748b', margin: '0 0 20px', lineHeight: 1.5 }}>
            Analyse sales orders, deliveries, and billing cycles.
          </p>
          <button style={{ ...btnStyle, background: '#006B3C' }}>Launch O2C</button>
        </div>

      </div>
    </div>

    <div style={{ textAlign: 'center', fontSize: 12, color: '#94a3b8', padding: '16px 0' }}>
      ©2026{' '}
      <a href="https://ajalabs.ai" target="_blank" rel="noopener noreferrer"
        style={{ color: '#64748b', textDecoration: 'none', fontWeight: 700 }}>
        ajaLabs.ai
      </a>{' '}
      All rights reserved
    </div>
  </div>
);

const cardStyle = {
  background: '#fff',
  padding: '2rem',
  borderRadius: 12,
  boxShadow: '0 4px 16px rgba(0,0,0,0.08)',
  cursor: 'pointer',
  width: 280,
  textAlign: 'center',
  transition: 'transform 0.2s, box-shadow 0.2s',
  border: '1px solid #e2e8f0',
};
const cardHover = {
  transform: 'translateY(-4px)',
  boxShadow: '0 10px 28px rgba(0,0,0,0.13)',
};
const btnStyle = {
  padding: '9px 0',
  color: '#fff',
  border: 'none',
  borderRadius: 6,
  cursor: 'pointer',
  fontWeight: 700,
  fontSize: 13,
  width: '100%',
};

/* ─── ROOT APP ────────────────────────────────────────────────────────────── */
export default function App() {
  const [currentUser,   setCurrentUser]   = useState(null);
  const [activeModule,  setActiveModule]  = useState(null); // null | 'p2p' | 'o2c'

  const handleLogin    = (u) => setCurrentUser(u);
  const handleSignOut  = () => { setCurrentUser(null); setActiveModule(null); };
  const handleSelect   = (mod) => setActiveModule(mod);
  const handleBackHome = () => setActiveModule(null);

  if (!currentUser) {
    return <LoginScreen onLogin={handleLogin} />;
  }

  if (activeModule === 'p2p') {
    return <P2PDashboard currentUser={currentUser} onSignOut={handleSignOut} onBackHome={handleBackHome} />;
  }

  if (activeModule === 'o2c') {
    return <O2CDashboard currentUser={currentUser} onSignOut={handleSignOut} onBackHome={handleBackHome} />;
  }

  return <ModuleSelector currentUser={currentUser} onSelect={handleSelect} onSignOut={handleSignOut} />;
}