import React, { useState, useEffect, useRef } from 'react';
import P2PDashboard from './p2p';
import O2CDashboard from './o2c';
import './App.css';
import './Login.css';

const API = 'http://localhost:8000';

/* ─── INTERACTIVE PROCESS MAP ───────────────────────────────────────────────── */
const NUM_NODES = 25;

const InteractiveProcessMap = ({ restrictToRight = false }) => {
  const containerRef = useRef(null);
  const nodesRef = useRef([]);
  const edgesRef = useRef([]);
  const baseNodes = useRef([]);
  const connections = useRef([]);
  const [init, setInit] = useState(false);

  useEffect(() => {
    // Generate base random nodes scattered across percentages
    const colors = ['#4ade80', '#38bdf8', '#facc15', '#f87171', '#a78bfa', '#fb923c'];
    const generatedNodes = Array.from({ length: NUM_NODES }).map((_, i) => ({
      id: i,
      x: restrictToRight ? 50 + Math.random() * 45 : 5 + Math.random() * 90,
      y: 5 + Math.random() * 90,
      vx: (Math.random() - 0.5) * 0.1,
      vy: (Math.random() - 0.5) * 0.1,
      color: colors[Math.floor(Math.random() * colors.length)],
      r: 6 + Math.random() * 10,
      pulseOffset: Math.random() * 4
    }));
    baseNodes.current = generatedNodes;

    // Connect each node to its 2 nearest neighbors
    const edges = [];
    for (let i = 0; i < NUM_NODES; i++) {
      const distances = generatedNodes.map((n, j) => ({
        index: j,
        dist: i === j ? Infinity : Math.hypot(n.x - generatedNodes[i].x, n.y - generatedNodes[i].y)
      }));
      distances.sort((a, b) => a.dist - b.dist);
      
      for (let k = 0; k < 2; k++) {
        const targetIdx = distances[k].index;
        // Avoid duplicate reverse edges
        if (!edges.some(e => (e.source === i && e.target === targetIdx) || (e.source === targetIdx && e.target === i))) {
          edges.push({ source: i, target: targetIdx, id: `${i}-${targetIdx}` });
        }
      }
    }
    connections.current = edges;
    setInit(true);
  }, []);

  useEffect(() => {
    if (!init) return;

    let animationFrameId;
    let targetX = -1000;
    let targetY = -1000;
    let currentX = -1000;
    let currentY = -1000;

    const updateDOM = (mouseX, mouseY) => {
      const rect = containerRef.current.getBoundingClientRect();
      const mouseXPct = ((mouseX - rect.left) / rect.width) * 100;
      const mouseYPct = ((mouseY - rect.top) / rect.height) * 100;

      const dynamicNodes = baseNodes.current.map(node => {
        // Continuous slow background drift
        node.x += node.vx;
        node.y += node.vy;
        
        // Bounce off boundaries
        if (restrictToRight) {
          if (node.x > 97 || node.x < 50) node.vx *= -1;
        } else {
          if (node.x > 97 || node.x < 3) node.vx *= -1;
        }
        if (node.y > 97 || node.y < 3) node.vy *= -1;

        let pullX = 0;
        let pullY = 0;
        
        // Only run magnet effect if mouse is on screen
        if (mouseX > 0) {
          const dx = mouseXPct - node.x;
          // Approximate aspect ratio correction (very rough, just for feel)
          const dy = (mouseYPct - node.y) * (rect.height / rect.width);
          const dist = Math.hypot(dx, dy);

          // Magnet radius of 15%
          if (dist < 15) {
            const power = Math.pow((15 - dist) / 15, 2); // Ease-in pull
            pullX = dx * power * 0.6; // Max 60% pull towards cursor
            pullY = (mouseYPct - node.y) * power * 0.6; 
          }
        }

        return { ...node, cx: node.x + pullX, cy: node.y + pullY };
      });

      // Update Native DOM
      dynamicNodes.forEach((node, i) => {
        if (nodesRef.current[i]) {
          nodesRef.current[i].setAttribute('cx', `${node.cx}%`);
          nodesRef.current[i].setAttribute('cy', `${node.cy}%`);
        }
      });

      connections.current.forEach((edge, i) => {
        if (edgesRef.current[i]) {
          const n1 = dynamicNodes[edge.source];
          const n2 = dynamicNodes[edge.target];
          edgesRef.current[i].setAttribute('x1', `${n1.cx}%`);
          edgesRef.current[i].setAttribute('y1', `${n1.cy}%`);
          edgesRef.current[i].setAttribute('x2', `${n2.cx}%`);
          edgesRef.current[i].setAttribute('y2', `${n2.cy}%`);
        }
      });
    };

    const handleMouseMove = (e) => {
      targetX = e.clientX;
      targetY = e.clientY;
      // Boot initial position immediately on entry
      if (currentX === -1000) {
        currentX = targetX;
        currentY = targetY;
      }
    };
    const handleMouseOut = () => {
      targetX = -1000;
      targetY = -1000;
    };

    window.addEventListener('mousemove', handleMouseMove);
    window.addEventListener('mouseout', handleMouseOut);

    const loop = () => {
      currentX += (targetX - currentX) * 0.08;
      currentY += (targetY - currentY) * 0.08;
      updateDOM(currentX, currentY);
      animationFrameId = requestAnimationFrame(loop);
    };
    loop();

    return () => {
      window.removeEventListener('mousemove', handleMouseMove);
      window.removeEventListener('mouseout', handleMouseOut);
      cancelAnimationFrame(animationFrameId);
    };
  }, [init]);

  if (!init) return null;

  return (
    <div className="process-bg-animation" ref={containerRef}>
      <svg width="100%" height="100%" style={{ position: 'absolute', top: 0, left: 0, overflow: 'visible' }}>
        {connections.current.map((edge, i) => {
          const sNode = baseNodes.current[edge.source];
          const edgeClass = `process-edge edge-${(i % 6) + 1}`;
          return (
            <line
              key={edge.id}
              ref={el => edgesRef.current[i] = el}
              x1={`${sNode.x}%`} y1={`${sNode.y}%`}
              x2={`${baseNodes.current[edge.target].x}%`} y2={`${baseNodes.current[edge.target].y}%`}
              className={edgeClass}
            />
          );
        })}

        {baseNodes.current.map((node, i) => (
          <circle
            key={node.id}
            ref={el => nodesRef.current[i] = el}
            cx={`${node.x}%`} cy={`${node.y}%`}
            r={node.r}
            className="process-node"
            style={{
              fill: `${node.color}22`,
              stroke: node.color,
              animationDelay: `${node.pulseOffset}s`
            }}
          />
        ))}
      </svg>
    </div>
  );
};

/* ─── SHARED LOGIN SCREEN ─────────────────────────────────────────────────── */
const LoginScreen = ({ onLogin }) => {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
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
    <div className="login-screen-bg">
      <InteractiveProcessMap restrictToRight={true} />
      
      <div className="login-content-wrapper">
        <div className="login-hero-info">
          <h1 className="hero-title">
            Uncover Hidden <br/>
            <span className="text-highlight">Inefficiencies.</span>
          </h1>
          <p className="hero-subtitle">
            Process mining is a data science technique that analyzes event log data from IT systems 
            (ERP, CRM) to visualize, analyze, and improve business processes. 
            It creates a data-driven map of actual operational workflows to identify bottlenecks, compliance issues, and inefficiencies.
            <br/>
            Transform your raw operational data into actionable insights.
            Visualize how your enterprise processes truly execute, uncover bottlenecks, and drive continuous improvement across your entire organization.
          </p>
          <div className="hero-features">
            <div className="feature-item">
              <div className="feature-icon-wrapper">⚡</div>
              <span>Real-time Process Discovery</span>
            </div>
            <div className="feature-item">
              <div className="feature-icon-wrapper">🔍</div>
              <span>Root Cause Analysis</span>
            </div>
            <div className="feature-item">
              <div className="feature-icon-wrapper">📈</div>
              <span>Continuous Optimization</span>
            </div>
          </div>
        </div>

        <div className="login-form-container">
          <div className="login-card">
            <img
              src="/logo.png"
              alt="ajaLabs Logo"
              onError={e => { e.target.style.display = 'none'; }}
              className="login-logo"
            />

            <h2 className="login-title">
              Welcome
            </h2>
        <p className="login-subtitle">
          Sign in to access your dashboard
        </p>

        {error && (
          <div className="login-error">
            {error}
          </div>
        )}

        <form onSubmit={handleSubmit} className="login-form">
          <div className="input-group">
            <input
              type="text"
              placeholder="Username"
              required
              value={username}
              onChange={e => setUsername(e.target.value)}
              className="glass-input"
            />
          </div>
          <div className="input-group" style={{ position: 'relative' }}>
            <input
              type={showPassword ? "text" : "password"}
              placeholder="Password"
              required
              value={password}
              onChange={e => setPassword(e.target.value)}
              className="glass-input"
              style={{ paddingRight: 40 }}
            />
            <button
              type="button"
              onClick={() => setShowPassword(!showPassword)}
              style={{
                position: 'absolute',
                right: 12,
                top: '50%',
                transform: 'translateY(-50%)',
                background: 'none',
                border: 'none',
                color: 'rgba(255,255,255,0.7)',
                cursor: 'pointer',
                fontSize: 14,
                padding: 4
              }}
              title={showPassword ? "Hide password" : "Show password"}
            >
              {showPassword ? "🙈" : "👁️"}
            </button>
          </div>
          <button
            type="submit"
            disabled={loading}
            className="login-btn"
          >
            {loading ? 'Signing in…' : 'Sign In'}
          </button>
        </form>
      </div>
      </div>
      </div>

      <div className="login-footer">
        ©2023{' '}
        <a href="https://ajalabs.ai" target="_blank" rel="noopener noreferrer" className="brand">
          ajalabs.ai
        </a>{' '}
        All rights reserved ·{' '}
        <a href="#" className="link">Data Privacy</a>
      </div>
    </div>
  );
};

/* ─── MODULE SELECTOR ─────────────────────────────────────────────────────── */
const ModuleSelector = ({ currentUser, onSelect, onSignOut }) => (
  <div className="login-screen-bg" style={{ display: 'block', overflowY: 'auto' }}>
    <InteractiveProcessMap restrictToRight={false} />
    <div style={{ position: 'relative', zIndex: 10, display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
    {/* Header */}
    <div style={{
      background: 'rgba(0, 0, 0, 0.2)', padding: '14px 28px',
      display: 'flex', justifyContent: 'space-between', alignItems: 'center',
      backdropFilter: 'blur(10px)',
      borderBottom: '1px solid rgba(255,255,255,0.1)'
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
          <div style={{ color: 'rgba(255,255,255,0.5)', fontSize: 11 }}>AJALabs Select a module</div>
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
      <h1 style={{ fontSize: '1.9rem', color: '#fff', margin: '0 0 0.5rem', fontWeight: 700 }}>
        Select a Module
      </h1>
      <p style={{ color: '#cbd5e1', fontSize: 14, margin: '0 0 3rem' }}>
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
          <h2 style={{ fontSize: '1.05rem', margin: '0 0 8px', color: '#fff', fontWeight: 700 }}>
            Procure-to-Pay
          </h2>
          <p style={{ fontSize: 13, color: '#94a3b8', margin: '0 0 20px', lineHeight: 1.5 }}>
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
          <h2 style={{ fontSize: '1.05rem', margin: '0 0 8px', color: '#fff', fontWeight: 700 }}>
            Order-to-Cash
          </h2>
          <p style={{ fontSize: 13, color: '#94a3b8', margin: '0 0 20px', lineHeight: 1.5 }}>
            Analyse sales orders, deliveries, and billing cycles.
          </p>
          <button style={{ ...btnStyle, background: '#006B3C' }}>Launch O2C</button>
        </div>

      </div>
    </div>

    <div style={{ textAlign: 'center', fontSize: 12, color: 'rgba(255,255,255,0.5)', padding: '16px 0' }}>
      ©2023{' '}
      <a href="https://ajalabs.ai" target="_blank" rel="noopener noreferrer"
        style={{ color: '#fff', textDecoration: 'none', fontWeight: 700 }}>
        ajalabs.ai
      </a>{' '}
      All rights reserved
    </div>
  </div>
  </div>
);

const cardStyle = {
  background: 'rgba(255, 255, 255, 0.05)',
  backdropFilter: 'blur(12px)',
  padding: '2rem',
  borderRadius: 12,
  boxShadow: '0 8px 32px rgba(0,0,0,0.2)',
  cursor: 'pointer',
  width: 280,
  textAlign: 'center',
  transition: 'transform 0.2s, box-shadow 0.2s, background 0.2s',
  border: '1px solid rgba(255,255,255,0.1)',
};
const cardHover = {
  transform: 'translateY(-4px)',
  boxShadow: '0 12px 40px rgba(0,0,0,0.4)',
  background: 'rgba(255, 255, 255, 0.08)',
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
