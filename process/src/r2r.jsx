import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import ReactFlow, {
  Background, Controls, MiniMap,
  Position, Handle
} from 'reactflow';
import 'reactflow/dist/style.css';
import {
  BarChart, Bar, LineChart, Line, PieChart, Pie,
  XAxis, YAxis, Tooltip as ReChartsTooltip, ResponsiveContainer, Cell, Legend,
  CartesianGrid
} from 'recharts';
import { motion, AnimatePresence, useScroll, useSpring } from 'framer-motion';
import html2canvas from 'html2canvas';
import './App.css';

const C = {
  teal: '#00828A', tealLight: '#E6F4F5', tealDark: '#005F66',
  slate: '#605E5C', bg: '#F0F2F5', card: '#FFFFFF', border: '#E1DFDD',
  orange: '#CA5010', green: '#107C10', red: '#D13438',
  headerBg: '#004D52', jkBlue: '#0057B7'
};

const ACCENT = ['#00828A', '#00A3AD', '#007077', '#005F66', '#004D52', '#003F44', '#002B2E'];

/* ─── LOADING OVERLAY ───────────────────────────────────────────────────────── */
const LoadingOverlay = ({ visible, progress, label }) => {
  if (!visible) return null;
  let activeStep = 1;
  if (progress > 10 && progress < 100) activeStep = 2;
  if (progress >= 100) activeStep = 3;
  const phases = [
    { num: 1, name: 'Processing Data' },
    { num: 2, name: 'Analysing Data' },
    { num: 3, name: 'Dashboard Created' }
  ];
  return (
    <div style={{ position: 'fixed', inset: 0, zIndex: 99999, background: 'rgba(27,42,74,0.92)', backdropFilter: 'blur(1px)', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContainer: 'center', gap: 36, justifyContent: 'center' }}>
      <style>{`@keyframes lo-pulse{0%,100%{opacity:1}50%{opacity:.45}}`}</style>
      <img src="/logo.png" alt="AJALabs Logo" style={{ height: 80, objectFit: 'contain', animation: 'lo-pulse 1.5s ease-in-out infinite' }} />
      <div style={{ display: 'flex', gap: 40, alignItems: 'center' }}>
        {phases.map((phase, index) => {
          const isActive = activeStep === phase.num;
          const isDone = activeStep > phase.num;
          const color = isActive || isDone ? '#00828A' : 'rgba(255,255,255,0.25)';
          return (
            <div key={phase.num} style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 10, position: 'relative' }}>
              {index > 0 && (<div style={{ position: 'absolute', right: '100%', top: 16, width: 40, height: 2, background: isDone || isActive ? '#00828A' : 'rgba(255,255,255,0.15)', marginRight: 10, transition: 'all 0.4s ease' }} />)}
              <div style={{ width: 32, height: 32, borderRadius: '50%', background: isDone ? '#00828A' : (isActive ? 'rgba(0,130,138,0.1)' : 'transparent'), border: `2px solid ${color}`, display: 'flex', alignItems: 'center', justifyContent: 'center', color: isDone ? '#fff' : color, fontWeight: 'bold', fontSize: 14, transition: 'all 0.3s ease', boxShadow: isActive ? '0 0 12px rgba(0,130,138,0.4)' : 'none' }}>
                {isDone ? '✓' : phase.num}
              </div>
              <div style={{ color: isActive || isDone ? '#fff' : 'rgba(255,255,255,0.4)', fontSize: 13, fontWeight: isActive ? 700 : 500, transition: 'all 0.3s ease', letterSpacing: 0.5 }}>{phase.name}</div>
            </div>
          );
        })}
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 8, width: 400 }}>
        <div style={{ width: '100%', background: 'rgba(255,255,255,.15)', borderRadius: 8, height: 6, overflow: 'hidden' }}>
          <div style={{ height: '100%', borderRadius: 8, transition: 'width .4s ease', background: 'linear-gradient(90deg,#00828A,#00A3AD)', width: `${progress}%`, boxShadow: '0 0 12px rgba(0,130,138,.6)' }} />
        </div>
        <div style={{ fontSize: 12, color: 'rgba(255,255,255,.6)' }}>{label}</div>
      </div>
    </div>
  );
};

/* ─── PROCESS NODE ──────────────────────────────────────────────────────────── */
const ProcessNode = React.memo(({ data }) => {
  const freq = data?.frequency || 0;
  const isHappy = data?.isHappy;
  const accentColor = isHappy ? '#107C10' : '#D13438';
  return (
    <div style={{
      background: 'rgba(30, 41, 59, 0.85)',
      backdropFilter: 'blur(10px)',
      border: `2px solid ${accentColor}`,
      borderRadius: 10,
      padding: '12px 16px',
      minWidth: 200,
      textAlign: 'center',
      color: '#fff',
      boxShadow: '0 4px 12px rgba(0,0,0,0.3)',
      position: 'relative'
    }}>
      <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 4, letterSpacing: '0.3px' }}>{data.label}</div>
      <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.6)' }}>
        <strong style={{ color: accentColor }}>{freq}</strong> cases
      </div>
      <Handle type="target" position={Position.Left} id="left-t" style={{ opacity: 0 }} />
      <Handle type="source" position={Position.Right} id="right-s" style={{ opacity: 0 }} />
      <Handle type="target" position={Position.Top} id="top-t" style={{ opacity: 0 }} />
      <Handle type="source" position={Position.Bottom} id="bottom-s" style={{ opacity: 0 }} />
    </div>
  );
});

const nodeTypes = { processNode: ProcessNode };

/* ─── FILTER COMPONENTS ─────────────────────────────────────────────────────── */
const FilterSelect = ({ label, value, options, onChange }) => (
  <div style={{ display: 'flex', flexDirection: 'column', gap: 3, flex: 1, minWidth: 150 }}>
    <label style={{ fontSize: 10, fontWeight: 700, color: '#323130', textTransform: 'uppercase', letterSpacing: .4 }}>{label}</label>
    <select value={value} onChange={e => onChange(e.target.value)} style={{ fontSize: 12, padding: '5px 8px', borderRadius: 4, width: '100%', border: value && value !== 'ALL' ? `1.5px solid ${C.teal}` : `1px solid ${C.border}`, background: value && value !== 'ALL' ? C.tealLight : C.card, color: '#323130', outline: 'none', cursor: 'pointer', fontWeight: value && value !== 'ALL' ? 700 : 'normal' }}>
      {(Array.isArray(options) ? options : ['ALL']).map(o => (<option key={o} value={o}>{o}</option>))}
    </select>
  </div>
);

const renderSkeleton = (type) => {
  if (type === 'pie') {
    return (
      <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', marginTop: 10 }}>
        <div className="chart-skeleton-bar" style={{ width: 140, height: 140, borderRadius: '50%', background: 'transparent', border: '30px solid #eaecef' }} />
      </div>
    );
  }
  if (type === 'bar-horizontal') {
    return (
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', justifyContent: 'space-around', gap: 12, marginTop: 10, paddingRight: 20 }}>
        {[85, 45, 65, 30, 75].map((w, i) => (
          <div key={i} className="chart-skeleton-bar" style={{ height: 20, width: `${w}%` }} />
        ))}
      </div>
    );
  }
  if (type === 'timeline') {
    return (
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 16, marginTop: 10 }}>
        {[1, 2, 3, 4].map((_, i) => (
          <div key={i} style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
            <div className="chart-skeleton-bar" style={{ width: 16, height: 16, borderRadius: '50%', flexShrink: 0 }} />
            <div className="chart-skeleton-bar" style={{ height: 12, width: `${60 + (i % 3) * 15}%` }} />
          </div>
        ))}
      </div>
    );
  }
  if (type === 'line') {
    return (
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', justifyContent: 'center', alignItems: 'center', marginTop: 10, position: 'relative' }}>
        <svg width="100%" height="80%" viewBox="0 0 100 50" preserveAspectRatio="none">
          <path d="M 0 40 Q 25 10 50 30 T 100 10" fill="none" stroke="#eaecef" strokeWidth="3" />
        </svg>
      </div>
    );
  }
  return (
    <div style={{ flex: 1, display: 'flex', alignItems: 'flex-end', gap: 8, marginTop: 10 }}>
      {[35, 65, 40, 80, 55, 90].map((h, i) => (
        <div key={i} className="chart-skeleton-bar" style={{ flex: 1, height: `${h}%` }} />
      ))}
    </div>
  );
};

const ChartCard = React.memo(({ title, subtitle, children, highlighted, onClear, style = {}, loading = false, skeletonType = 'bar-vertical' }) => (
  <div style={{ background: C.card, borderRadius: 8, padding: '12px 14px', border: highlighted ? `1.5px solid ${C.teal}` : `1px solid ${C.border}`, boxShadow: '0 2px 8px rgba(0,0,0,.05)', transition: 'all .2s', display: 'flex', flexDirection: 'column', ...style }}>
    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 8 }}>
      <div>
        <div style={{ fontSize: 13, fontWeight: 700, color: '#323130' }}>{title}</div>
        {subtitle && <div style={{ fontSize: 10, color: '#8A8886', marginTop: 2 }}>{subtitle}</div>}
      </div>
      {highlighted && onClear && (<button onClick={onClear} style={{ fontSize: 11, color: '#fff', background: C.teal, border: 'none', borderRadius: 4, padding: '3px 9px', cursor: 'pointer', fontWeight: 600, flexShrink: 0 }}>Clear</button>)}
    </div>
    <div style={{ flex: 1, minHeight: 0, position: 'relative' }}>
      {loading && (
        <div className="chart-skeleton-container" style={{ position: 'absolute', inset: 0, zIndex: 10, background: C.card, display: 'flex', flexDirection: 'column', gap: 12, padding: 10 }}>
          <div className="chart-skeleton-bar" style={{ width: '40%', height: 14 }} />
          {renderSkeleton(skeletonType)}
        </div>
      )}
      <div style={{ opacity: loading ? 0 : 1, transition: 'opacity 0.3s', height: '100%' }}>{children}</div>
    </div>
  </div>
));

/* ─── MOCK DATABASE ─────────────────────────────────────────────────────────── */
const RAW_EVENTS = [
  // Case 1: Happy Path
  { case_id: 'R2R-101', activity: 'Journal Entry Created', timestamp: '2026-04-01 09:00', user: 'Sarah', status: 'Happy Path', month: 'April' },
  { case_id: 'R2R-101', activity: 'Journal Entry Approved', timestamp: '2026-04-01 11:30', user: 'Michael', status: 'Happy Path', month: 'April' },
  { case_id: 'R2R-101', activity: 'Posted to Ledger', timestamp: '2026-04-01 14:00', user: 'System', status: 'Happy Path', month: 'April' },
  { case_id: 'R2R-101', activity: 'Intercompany Reconciliation', timestamp: '2026-04-02 10:00', user: 'David', status: 'Happy Path', month: 'April' },
  { case_id: 'R2R-101', activity: 'Trial Balance Generated', timestamp: '2026-04-03 16:00', user: 'System', status: 'Happy Path', month: 'April' },
  { case_id: 'R2R-101', activity: 'Financial Reporting Completed', timestamp: '2026-04-05 17:00', user: 'Emma', status: 'Happy Path', month: 'April' },

  // Case 2: Happy Path
  { case_id: 'R2R-102', activity: 'Journal Entry Created', timestamp: '2026-04-02 10:15', user: 'Sarah', status: 'Happy Path', month: 'April' },
  { case_id: 'R2R-102', activity: 'Journal Entry Approved', timestamp: '2026-04-02 15:45', user: 'Michael', status: 'Happy Path', month: 'April' },
  { case_id: 'R2R-102', activity: 'Posted to Ledger', timestamp: '2026-04-02 17:30', user: 'System', status: 'Happy Path', month: 'April' },
  { case_id: 'R2R-102', activity: 'Intercompany Reconciliation', timestamp: '2026-04-03 11:00', user: 'David', status: 'Happy Path', month: 'April' },
  { case_id: 'R2R-102', activity: 'Trial Balance Generated', timestamp: '2026-04-04 15:00', user: 'System', status: 'Happy Path', month: 'April' },
  { case_id: 'R2R-102', activity: 'Financial Reporting Completed', timestamp: '2026-04-05 16:30', user: 'Emma', status: 'Happy Path', month: 'April' },

  // Case 3: Rejected & Reworked Deviation
  { case_id: 'R2R-103', activity: 'Journal Entry Created', timestamp: '2026-04-03 09:30', user: 'John', status: 'Deviation', month: 'April' },
  { case_id: 'R2R-103', activity: 'Rejected & Reworked', timestamp: '2026-04-03 14:00', user: 'Michael', status: 'Deviation', month: 'April' },
  { case_id: 'R2R-103', activity: 'Journal Entry Created', timestamp: '2026-04-04 09:00', user: 'John', status: 'Deviation', month: 'April' },
  { case_id: 'R2R-103', activity: 'Journal Entry Approved', timestamp: '2026-04-04 11:00', user: 'Michael', status: 'Deviation', month: 'April' },
  { case_id: 'R2R-103', activity: 'Posted to Ledger', timestamp: '2026-04-04 13:30', user: 'System', status: 'Deviation', month: 'April' },
  { case_id: 'R2R-103', activity: 'Intercompany Reconciliation', timestamp: '2026-04-04 16:00', user: 'David', status: 'Deviation', month: 'April' },
  { case_id: 'R2R-103', activity: 'Trial Balance Generated', timestamp: '2026-04-05 10:00', user: 'System', status: 'Deviation', month: 'April' },
  { case_id: 'R2R-103', activity: 'Financial Reporting Completed', timestamp: '2026-04-05 15:00', user: 'Emma', status: 'Deviation', month: 'April' },

  // Case 4: Manual Correction Deviation
  { case_id: 'R2R-104', activity: 'Journal Entry Created', timestamp: '2026-04-04 11:00', user: 'Sarah', status: 'Deviation', month: 'April' },
  { case_id: 'R2R-104', activity: 'Journal Entry Approved', timestamp: '2026-04-04 13:00', user: 'Michael', status: 'Deviation', month: 'April' },
  { case_id: 'R2R-104', activity: 'Posted to Ledger', timestamp: '2026-04-04 15:00', user: 'System', status: 'Deviation', month: 'April' },
  { case_id: 'R2R-104', activity: 'Manual Correction', timestamp: '2026-04-05 09:00', user: 'Sarah', status: 'Deviation', month: 'April' },
  { case_id: 'R2R-104', activity: 'Intercompany Reconciliation', timestamp: '2026-04-05 14:00', user: 'David', status: 'Deviation', month: 'April' },
  { case_id: 'R2R-104', activity: 'Trial Balance Generated', timestamp: '2026-04-06 11:00', user: 'System', status: 'Deviation', month: 'April' },
  { case_id: 'R2R-104', activity: 'Financial Reporting Completed', timestamp: '2026-04-07 10:00', user: 'Emma', status: 'Deviation', month: 'April' },

  // Case 5: Happy Path
  { case_id: 'R2R-105', activity: 'Journal Entry Created', timestamp: '2026-04-05 10:00', user: 'John', status: 'Happy Path', month: 'April' },
  { case_id: 'R2R-105', activity: 'Journal Entry Approved', timestamp: '2026-04-05 12:00', user: 'Michael', status: 'Happy Path', month: 'April' },
  { case_id: 'R2R-105', activity: 'Posted to Ledger', timestamp: '2026-04-05 14:00', user: 'System', status: 'Happy Path', month: 'April' },
  { case_id: 'R2R-105', activity: 'Intercompany Reconciliation', timestamp: '2026-04-06 09:30', user: 'David', status: 'Happy Path', month: 'April' },
  { case_id: 'R2R-105', activity: 'Trial Balance Generated', timestamp: '2026-04-06 15:00', user: 'System', status: 'Happy Path', month: 'April' },
  { case_id: 'R2R-105', activity: 'Financial Reporting Completed', timestamp: '2026-04-07 11:30', user: 'Emma', status: 'Happy Path', month: 'April' },

  // Case 6: Happy Path (May)
  { case_id: 'R2R-106', activity: 'Journal Entry Created', timestamp: '2026-05-01 09:00', user: 'John', status: 'Happy Path', month: 'May' },
  { case_id: 'R2R-106', activity: 'Journal Entry Approved', timestamp: '2026-05-01 11:00', user: 'Michael', status: 'Happy Path', month: 'May' },
  { case_id: 'R2R-106', activity: 'Posted to Ledger', timestamp: '2026-05-01 13:00', user: 'System', status: 'Happy Path', month: 'May' },
  { case_id: 'R2R-106', activity: 'Intercompany Reconciliation', timestamp: '2026-05-02 10:00', user: 'David', status: 'Happy Path', month: 'May' },
  { case_id: 'R2R-106', activity: 'Trial Balance Generated', timestamp: '2026-05-02 16:00', user: 'System', status: 'Happy Path', month: 'May' },
  { case_id: 'R2R-106', activity: 'Financial Reporting Completed', timestamp: '2026-05-03 14:00', user: 'Emma', status: 'Happy Path', month: 'May' }
];

const R2R_LAYOUT = {
  'Journal Entry Created': { step: 0, lane: 0 },
  'Journal Entry Approved': { step: 1, lane: 0 },
  'Posted to Ledger': { step: 2, lane: 0 },
  'Intercompany Reconciliation': { step: 3, lane: 0 },
  'Trial Balance Generated': { step: 4, lane: 0 },
  'Financial Reporting Completed': { step: 5, lane: 0 },
  'Rejected & Reworked': { step: 1, lane: -1 },
  'Manual Correction': { step: 3, lane: -1 }
};

/* ── FAQ Accordion Item ── */
const FaqItem = ({ q, a, bullets, accentColor }) => {
  const [open, setOpen] = useState(false);
  const accent = accentColor || '#00828A';
  return (
    <div style={{ borderBottom: '1px solid #E2E8F0' }}>
      <button
        onClick={() => setOpen(o => !o)}
        style={{
          width: '100%', display: 'flex', justifyContent: 'space-between', alignItems: 'center',
          padding: '14px 0', background: 'none', border: 'none', cursor: 'pointer', textAlign: 'left', gap: 12,
        }}>
        <span style={{ fontSize: 13.5, fontWeight: 600, color: '#1e293b', lineHeight: 1.4 }}>{q}</span>
        <span style={{
          fontSize: 18, color: accent, flexShrink: 0, fontWeight: 700,
          transform: open ? 'rotate(45deg)' : 'none', transition: 'transform 0.2s',
          display: 'inline-block', width: 20, textAlign: 'center',
        }}>+</span>
      </button>
      {open && (
        <div style={{ paddingBottom: 16, fontSize: 13, color: '#475569', lineHeight: 1.6 }}>
          <p style={{ margin: '0 0 10px 0' }}>{a}</p>
          {bullets && bullets.length > 0 && (
            <ul style={{ margin: 0, paddingLeft: 20, display: 'flex', flexDirection: 'column', gap: 6 }}>
              {bullets.map((b, i) => (
                <li key={i}>
                  {typeof b === 'object' ? (
                    <span><strong>{b.bold}</strong>{b.rest}</span>
                  ) : (
                    <span>{b}</span>
                  )}
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  );
};

const R2R_FAQS = [
  {
    q: 'What is the Record-to-Report (R2R) process?',
    a: 'The R2R process is the finance and accounting lifecycle that covers collecting, processing, and presenting accurate financial data. It runs from journal entry creation, ledger posting, and reconciliation, to generating the trial balance and publishing final compliance reports.',
    bullets: [
      { bold: 'Journal Entry Creation:', rest: ' Recording transactions in the ledger.' },
      { bold: 'Approvals & Controls:', rest: ' Verifying entries against compliance rules.' },
      { bold: 'General Ledger Posting:', rest: ' Finalizing entries in the ERP core.' },
      { bold: 'Intercompany Reconciliation:', rest: ' Matching accounts across group entities.' },
      { bold: 'Financial Reporting:', rest: ' Consolidated statements for external review.' },
    ],
  },
  {
    q: 'What are the key R2R pain points process mining uncovers?',
    a: 'Process mining analyzes log data to expose inefficiencies in the closing cycle and internal control violations:',
    bullets: [
      'Excessive manual journal entry adjustments causing human error',
      'Long closing delays due to intercompany discrepancies',
      'Late period adjustments causing reporting lags',
      'Unapproved or bypassed journal entry workflows',
    ],
  },
  {
    q: 'What key metrics (KPIs) does Process Mining help track in R2R?',
    a: 'R2R process mining monitors critical cycle time and quality metrics:',
    bullets: [
      { bold: 'Journal Entry Latency:', rest: ' Time to post entries after transaction.' },
      { bold: 'Intercompany Match Rate:', rest: ' % of matched transactions on first run.' },
      { bold: 'Close-to-Report Lead Time:', rest: ' Total days to close period and report.' },
      { bold: 'Auto-posting Success Rate:', rest: ' % of automated entries posted without manual intervention.' },
    ],
  }
];

const KpiWheel = ({ colors, label, height = 302, width = 140 }) => {
  const cx = 0;
  const cy = height / 2;
  const rOut = 95;
  const rIn = 65;

  const segments = [];
  for (let i = 0; i < 6; i++) {
    const startAngle = -90 + i * 30 + 1.5;
    const endAngle = -90 + (i + 1) * 30 - 1.5;

    const rad1 = (startAngle * Math.PI) / 180;
    const rad2 = (endAngle * Math.PI) / 180;

    const x1_out = cx + rOut * Math.cos(rad1);
    const y1_out = cy + rOut * Math.sin(rad1);
    const x2_out = cx + rOut * Math.cos(rad2);
    const y2_out = cy + rOut * Math.sin(rad2);

    const x1_in = cx + rIn * Math.cos(rad1);
    const y1_in = cy + rIn * Math.sin(rad1);
    const x2_in = cx + rIn * Math.cos(rad2);
    const y2_in = cy + rIn * Math.sin(rad2);

    const d = `
      M ${x1_in} ${y1_in}
      L ${x1_out} ${y1_out}
      A ${rOut} ${rOut} 0 0 1 ${x2_out} ${y2_out}
      L ${x2_in} ${y2_in}
      A ${rIn} ${rIn} 0 0 0 ${x1_in} ${y1_in}
      Z
    `;
    segments.push({ d, color: colors[i] });
  }

  return (
    <svg width={width} height={height} style={{ overflow: 'visible', flexShrink: 0 }}>
      {segments.map((seg, idx) => (
        <path key={idx} d={seg.d} fill={seg.color} opacity={0.85} />
      ))}

      <path d={`M 0 ${cy - rOut} A ${rOut} ${rOut} 0 0 1 0 ${cy + rOut}`} fill="none" stroke="#E2E8F0" strokeWidth="1" />
      <path d={`M 0 ${cy - rIn} A ${rIn} ${rIn} 0 0 1 0 ${cy + rIn} Z`} fill="#FFFFFF" />

      <text x="6" y={cy - 12} fill="#64748b" style={{ fontSize: '8px', fontWeight: 800, letterSpacing: '1px' }}>KEY</text>
      <text x="6" y={cy + 2} fill="#1e293b" style={{ fontSize: '11px', fontWeight: 900, letterSpacing: '0.5px' }}>{label}</text>
      <text x="6" y={cy + 14} fill="#64748b" style={{ fontSize: '8px', fontWeight: 800, letterSpacing: '1px' }}>KPIs</text>

      {colors.map((color, i) => {
        const angle = -90 + i * 30 + 15;
        const rad = (angle * Math.PI) / 180;
        const sx = cx + rOut * Math.cos(rad);
        const sy = cy + rOut * Math.sin(rad);
        const tx = width + 5;
        const ty = i * 50 + 22;

        const pathD = `M ${sx} ${sy} C ${sx + 35} ${sy}, ${tx - 35} ${ty}, ${tx} ${ty}`;

        return (
          <g key={i}>
            <path
              d={pathD}
              fill="none"
              stroke={color}
              strokeWidth="1.5"
              strokeDasharray="3 3"
              opacity={0.5}
            />
            <circle cx={sx} cy={sy} r="3" fill={color} />
          </g>
        );
      })}
    </svg>
  );
};

export default function R2RDashboard({ currentUser, onSignOut, onBackHome }) {
  const [step, setStep] = useState('info'); // 'info' | 'choose' | 'dashboard'
  const [loading, setLoading] = useState(false);
  const [loadProg, setLoadProg] = useState(0);
  const [loadLabel, setLoadLabel] = useState('Initializing R2R module...');
  const [showAvailablePopup, setShowAvailablePopup] = useState(false);
  const [hoveredSide, setHoveredSide] = useState(null);
  const [showFaqModal, setShowFaqModal] = useState(false);
  const [chartsLoading, setChartsLoading] = useState(true);

  useEffect(() => {
    if (step === 'dashboard') {
      setChartsLoading(true);
      const timer = setTimeout(() => {
        setChartsLoading(false);
      }, 1200);
      return () => clearTimeout(timer);
    }
  }, [step]);

  useEffect(() => {
    const current = window.history.state;
    if (current && current.activeModule === 'r2r') {
      if (current.step !== undefined) setStep(current.step);
    }

    const handlePopState = (event) => {
      if (event.state && event.state.activeModule === 'r2r') {
        if (event.state.step !== undefined) setStep(event.state.step);
      }
    };

    window.addEventListener('popstate', handlePopState);
    return () => window.removeEventListener('popstate', handlePopState);
  }, []);

  useEffect(() => {
    const current = window.history.state;
    if (!current || current.activeModule !== 'r2r' || current.step !== step) {
      window.history.pushState({ activeModule: 'r2r', step }, '');
    }
  }, [step]);

  const screenshotRef = useRef(null);
  const [screenshotting, setScreenshotting] = useState(false);

  const handleScreenshot = async () => {
    if (!screenshotRef.current || screenshotting) return;
    setScreenshotting(true);
    const el = screenshotRef.current;

    const originalRootStyle = {
      height: el.style.height,
      overflowX: el.style.overflowX,
      overflowY: el.style.overflowY,
      maxHeight: el.style.maxHeight,
    };

    el.style.setProperty('height', 'auto', 'important');
    el.style.setProperty('overflow-x', 'visible', 'important');
    el.style.setProperty('overflow-y', 'visible', 'important');
    el.style.setProperty('max-height', 'none', 'important');

    const scrollContainers = [];
    const allElements = el.querySelectorAll('div[style*="overflow"]');
    allElements.forEach((node) => {
      const style = window.getComputedStyle(node);
      if (
        style.overflowY === 'auto' ||
        style.overflowY === 'scroll' ||
        style.overflowX === 'auto' ||
        style.overflowX === 'scroll' ||
        style.overflow === 'auto' ||
        style.overflow === 'scroll'
      ) {
        scrollContainers.push({
          node,
          overflowX: node.style.overflowX,
          overflowY: node.style.overflowY,
          height: node.style.height,
          maxHeight: node.style.maxHeight,
        });
        node.style.setProperty('overflow-x', 'visible', 'important');
        node.style.setProperty('overflow-y', 'visible', 'important');
        node.style.setProperty('height', 'auto', 'important');
        node.style.setProperty('max-height', 'none', 'important');
      }
    });

    try {
      await new Promise((resolve) => requestAnimationFrame(() => setTimeout(resolve, 100)));

      const canvas = await html2canvas(el, {
        scale: 1,
        useCORS: true,
        allowTaint: false,
        backgroundColor: '#F0F2F5',
        logging: false,
        scrollX: 0,
        scrollY: 0,
        windowWidth: el.scrollWidth,
        windowHeight: el.scrollHeight,
        width: el.scrollWidth,
        height: el.scrollHeight,
        ignoreElements: (node) => {
          if (node.id && node.id.includes('screenshot-btn')) return true;
          if (node.classList && (node.classList.contains('no-screenshot') || node.classList.contains('screenshot-btn'))) return true;
          return false;
        },
        onclone: (clonedDoc) => {
          const copyStyles = (selector) => {
            const originalEls = el.querySelectorAll(selector);
            const clonedEls = clonedDoc.querySelectorAll(selector);
            for (let i = 0; i < originalEls.length && i < clonedEls.length; i++) {
              const oEl = originalEls[i];
              const cEl = clonedEls[i];
              const oStyle = window.getComputedStyle(oEl);

              // Copy layout & positioning styles
              cEl.style.position = oStyle.position;
              cEl.style.width = oStyle.width;
              cEl.style.height = oStyle.height;
              cEl.style.top = oStyle.top;
              cEl.style.left = oStyle.left;
              cEl.style.transform = oStyle.transform;
              cEl.style.transformOrigin = oStyle.transformOrigin;
              cEl.style.display = oStyle.display;
              cEl.style.opacity = oStyle.opacity;
              cEl.style.overflow = oStyle.overflow;

              // SVG specific styles
              if (oStyle.stroke) cEl.style.stroke = oStyle.stroke;
              if (oStyle.strokeWidth) cEl.style.strokeWidth = oStyle.strokeWidth;
              if (oStyle.fill) cEl.style.fill = oStyle.fill;

              // Styling details
              cEl.style.background = oStyle.background;
              cEl.style.backgroundColor = oStyle.backgroundColor;
              cEl.style.color = oStyle.color;
              cEl.style.border = oStyle.border;
              cEl.style.borderRadius = oStyle.borderRadius;
              cEl.style.boxShadow = oStyle.boxShadow;
              cEl.style.padding = oStyle.padding;
            }
          };

          copyStyles('.react-flow__renderer');
          copyStyles('.react-flow__viewport');
          copyStyles('.react-flow__edges');
          copyStyles('.react-flow__nodes');
          copyStyles('.react-flow__edge');
          copyStyles('.react-flow__edge-path');
          copyStyles('.react-flow__node');
          copyStyles('.react-flow__background');
          copyStyles('.react-flow__container');
        }
      });

      await new Promise((resolve, reject) => {
        canvas.toBlob((blob) => {
          if (!blob) {
            reject(new Error('Canvas toBlob returned null'));
            return;
          }
          const url = URL.createObjectURL(blob);
          const link = document.createElement('a');
          link.download = `R2R_Dashboard_Report_${new Date().toISOString().slice(0, 10)}.png`;
          link.href = url;
          document.body.appendChild(link);
          link.click();
          document.body.removeChild(link);
          setTimeout(() => {
            URL.revokeObjectURL(url);
          }, 1000);
          resolve();
        }, 'image/png');
      });
    } catch (err) {
      console.error('Screenshot failed:', err);
    } finally {
      // Restore root element style
      if (originalRootStyle.height) el.style.setProperty('height', originalRootStyle.height);
      else el.style.removeProperty('height');

      if (originalRootStyle.overflowX) el.style.setProperty('overflow-x', originalRootStyle.overflowX);
      else el.style.removeProperty('overflow-x');

      if (originalRootStyle.overflowY) el.style.setProperty('overflow-y', originalRootStyle.overflowY);
      else el.style.removeProperty('overflow-y');

      if (originalRootStyle.maxHeight) el.style.setProperty('max-height', originalRootStyle.maxHeight);
      else el.style.removeProperty('max-height');

      // Restore scroll containers style
      scrollContainers.forEach(({ node, overflowX, overflowY, height, maxHeight }) => {
        if (overflowX) node.style.setProperty('overflow-x', overflowX);
        else node.style.removeProperty('overflow-x');

        if (overflowY) node.style.setProperty('overflow-y', overflowY);
        else node.style.removeProperty('overflow-y');

        if (height) node.style.setProperty('height', height);
        else node.style.removeProperty('height');

        if (maxHeight) node.style.setProperty('max-height', maxHeight);
        else node.style.removeProperty('max-height');
      });

      setScreenshotting(false);
    }
  };

  const [selected, setSelected] = useState({
    case_id: 'ALL',
    user: 'ALL',
    status: 'ALL',
    month: 'ALL'
  });
  const [layoutDir, setLayoutDir] = useState('LR');

  // Trigger loading phase on clicking "Load Demo Dataset"
  const handleLoadDemo = () => {
    setLoading(true);
    setLoadProg(0);
    setLoadLabel('Initializing Record to Report module...');

    let timer;
    const updateProgress = () => {
      setLoadProg(prev => {
        if (prev >= 100) {
          clearInterval(timer);
          setLoading(false);
          setStep('dashboard');
          return 100;
        }
        const next = prev + Math.floor(Math.random() * 20) + 10;
        if (next > 40 && next < 80) setLoadLabel('Analysing journal entry steps and posting cycles...');
        if (next >= 80 && next < 100) setLoadLabel('Building financial process map...');
        if (next >= 100) {
          setLoadLabel('Dashboard created successfully!');
          return 100;
        }
        return next;
      });
    };
    timer = setInterval(updateProgress, 180);
  };

  // Filter calculations
  const filteredEvents = useMemo(() => {
    return RAW_EVENTS.filter(e => {
      if (selected.case_id !== 'ALL' && e.case_id !== selected.case_id) return false;
      if (selected.user !== 'ALL' && e.user !== selected.user) return false;
      if (selected.status !== 'ALL' && e.status !== selected.status) return false;
      if (selected.month !== 'ALL' && e.month !== selected.month) return false;
      return true;
    });
  }, [selected]);

  // Group events by Case ID
  const casesMap = useMemo(() => {
    const map = {};
    filteredEvents.forEach(e => {
      if (!map[e.case_id]) map[e.case_id] = [];
      map[e.case_id].push(e);
    });
    Object.keys(map).forEach(cid => {
      map[cid].sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
    });
    return map;
  }, [filteredEvents]);

  // Unique lists for slicers
  const slicerOptions = useMemo(() => {
    const cids = [...new Set(RAW_EVENTS.map(e => e.case_id))];
    const users = [...new Set(RAW_EVENTS.map(e => e.user))];
    const statuses = [...new Set(RAW_EVENTS.map(e => e.status))];
    const months = [...new Set(RAW_EVENTS.map(e => e.month))];
    return {
      case_id: ['ALL', ...cids],
      user: ['ALL', ...users],
      status: ['ALL', ...statuses],
      month: ['ALL', ...months]
    };
  }, []);

  // Node & Edge frequencies + Durations
  const graphData = useMemo(() => {
    const nodeFreq = {};
    const edgeFreq = {};
    const edgeDurations = {};

    Object.keys(casesMap).forEach(cid => {
      const caseEvents = casesMap[cid];
      for (let i = 0; i < caseEvents.length; i++) {
        const act = caseEvents[i].activity;
        nodeFreq[act] = (nodeFreq[act] || 0) + 1;

        if (i > 0) {
          const prevAct = caseEvents[i - 1].activity;
          const key = `${prevAct}→${act}`;
          edgeFreq[key] = (edgeFreq[key] || 0) + 1;

          const diffMs = new Date(caseEvents[i].timestamp) - new Date(caseEvents[i - 1].timestamp);
          const diffDays = diffMs / (1000 * 60 * 60 * 24);
          if (!edgeDurations[key]) edgeDurations[key] = [];
          edgeDurations[key].push(diffDays);
        }
      }
    });

    return { nodeFreq, edgeFreq, edgeDurations };
  }, [casesMap]);

  // Build ReactFlow Nodes and Edges
  const { rfNodes, rfEdges } = useMemo(() => {
    const stepGap = 280;
    const laneGap = 165;
    const nodes = Object.keys(graphData.nodeFreq).map(act => {
      const layout = R2R_LAYOUT[act] || { step: 0, lane: 2 };
      const isHappy = !['Rejected & Reworked', 'Manual Correction'].includes(act);
      const x = layout.step * stepGap + 40;
      const y = layout.lane * laneGap + 180;
      return {
        id: act,
        type: 'processNode',
        position: layoutDir === 'LR' ? { x, y } : { x: y, y: x },
        data: {
          label: act,
          frequency: graphData.nodeFreq[act],
          isHappy
        }
      };
    });

    const edges = Object.keys(graphData.edgeFreq).map(key => {
      const [src, tgt] = key.split('→');
      const freq = graphData.edgeFreq[key];
      const durations = graphData.edgeDurations[key] || [0];
      const avgDays = (durations.reduce((a, b) => a + b, 0) / durations.length).toFixed(1);

      const isHappyEdge = !['Rejected & Reworked', 'Manual Correction'].includes(src) &&
        !['Rejected & Reworked', 'Manual Correction'].includes(tgt);
      const color = isHappyEdge ? '#107C10' : '#D13438';

      return {
        id: key,
        source: src,
        target: tgt,
        animated: true,
        style: { stroke: color, strokeWidth: 2.5 },
        sourceHandle: layoutDir === 'LR' ? 'right-s' : 'bottom-s',
        targetHandle: layoutDir === 'LR' ? 'left-t' : 'top-t',
        label: `${freq}x (${avgDays}d)`,
        labelStyle: { fill: '#ffffff', fontSize: 10, fontWeight: 600 },
        labelBgStyle: { fill: 'rgba(15, 23, 42, 0.85)', fillOpacity: 0.9, rx: 4, ry: 4 },
      };
    });

    return { rfNodes: nodes, rfEdges: edges };
  }, [graphData, layoutDir]);

  // KPI calculations
  const kpis = useMemo(() => {
    const totalCases = Object.keys(casesMap).length;
    let completedCount = 0;
    let totalDays = 0;
    let reworkCases = 0;
    let manualCorrectionCases = 0;

    Object.keys(casesMap).forEach(cid => {
      const cEvents = casesMap[cid];
      const acts = cEvents.map(e => e.activity);
      if (acts.includes('Financial Reporting Completed')) completedCount++;
      if (acts.includes('Rejected & Reworked')) reworkCases++;
      if (acts.includes('Manual Correction')) manualCorrectionCases++;

      if (cEvents.length > 1) {
        const start = new Date(cEvents[0].timestamp);
        const end = new Date(cEvents[cEvents.length - 1].timestamp);
        totalDays += (end - start) / (1000 * 60 * 60 * 24);
      }
    });

    const avgCycle = totalCases ? (totalDays / totalCases).toFixed(1) : '0';
    const rejectionRate = totalCases ? ((reworkCases / totalCases) * 100).toFixed(0) : '0';
    const manualRate = totalCases ? ((manualCorrectionCases / totalCases) * 100).toFixed(0) : '0';

    return {
      cases: totalCases,
      cycle: `${avgCycle} Days`,
      rework: `${rejectionRate}%`,
      manual: `${manualRate}%`
    };
  }, [casesMap]);

  // Chart: Happy path vs deviations
  const donutData = useMemo(() => {
    let happy = 0;
    let dev = 0;
    Object.keys(casesMap).forEach(cid => {
      const acts = casesMap[cid].map(e => e.activity);
      if (acts.includes('Rejected & Reworked') || acts.includes('Manual Correction')) {
        dev++;
      } else {
        happy++;
      }
    });
    return [
      { name: 'Happy Path', value: happy, color: '#107C10' },
      { name: 'Deviations', value: dev, color: '#D13438' }
    ];
  }, [casesMap]);

  // Chart: Activity Frequency
  const barChartData = useMemo(() => {
    return Object.keys(graphData.nodeFreq).map(act => ({
      name: act,
      count: graphData.nodeFreq[act]
    })).sort((a, b) => b.count - a.count);
  }, [graphData]);

  // Chart: Monthly postings volume
  const monthlyData = useMemo(() => {
    const months = {};
    RAW_EVENTS.forEach(e => {
      if (e.activity === 'Journal Entry Created') {
        months[e.month] = (months[e.month] || 0) + 1;
      }
    });
    return Object.keys(months).map(m => ({
      month: m,
      volume: months[m]
    }));
  }, []);

  // Case table row select
  const [selectedCaseId, setSelectedCaseId] = useState('ALL');
  const activeCaseEvents = useMemo(() => {
    if (selectedCaseId === 'ALL') return [];
    return RAW_EVENTS.filter(e => e.case_id === selectedCaseId)
      .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  }, [selectedCaseId]);

  const resetAll = () => {
    setSelected({
      case_id: 'ALL',
      user: 'ALL',
      status: 'ALL',
      month: 'ALL'
    });
    setSelectedCaseId('ALL');
  };

  /* ── R2R Intro helpers ── */
  const r2rSteps = [
    'Journal Entry Created — Posting documents initialized manually or automatically.',
    'Journal Entry Approved — Workflow approvals verified against internal control limits.',
    'Posted to Ledger — General ledger entry finalized in ERP core modules.',
    'Intercompany Reconciliation — Matching assets, liabilities, and balances across group entities.',
    'Trial Balance Generated — Generating ledger consolidations for financial verification.',
    'Financial Reporting Completed — Final report output ready for compliance and review.',
  ];
  const r2rKpis = [
    'Journal entry posting latency',
    'Intercompany reconciliation match rate',
    'Close-to-report lead time',
    'Manual journal entry correction %',
    'Auto-posting success rate',
    'Late adjustment volume',
  ];
  const r2rShortSteps = [
    { icon: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" /><polyline points="14 2 14 8 20 8" /></svg>, text: 'JE' },
    { icon: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14" /><polyline points="22 4 12 14.01 9 11.01" /></svg>, text: 'Approve' },
    { icon: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M4 2v20l2-1 2 1 2-1 2 1 2-1 2 1 2-1 2 1V2l-2 1-2-1-2 1-2-1-2 1-2-1-2 1-2-1z" /></svg>, text: 'Ledger' },
    { icon: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><line x1="8" y1="6" x2="21" y2="6" /><line x1="8" y1="12" x2="21" y2="12" /><line x1="8" y1="18" x2="21" y2="18" /><line x1="3" y1="6" x2="3.01" y2="6" /><line x1="3" y1="12" x2="3.01" y2="12" /><line x1="3" y1="18" x2="3.01" y2="18" /></svg>, text: 'Reconcile' },
    { icon: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><rect x="1" y="4" width="22" height="16" rx="2" ry="2" /><line x1="1" y1="10" x2="23" y2="10" /></svg>, text: 'Balance' },
    { icon: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12" /></svg>, text: 'Report' },
  ];
  const R2RPremiumMetricAnimation = ({ num }) => (
    <div style={{ position: 'relative', width: '60px', height: '60px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
      <motion.div animate={{ scale: [1, 1.6], opacity: [0.6, 0] }} transition={{ duration: 2, repeat: Infinity, ease: 'easeOut' }} style={{ position: 'absolute', width: '40px', height: '40px', borderRadius: '50%', border: '2px solid rgba(255,255,255,0.8)' }} />
      <motion.div animate={{ scale: [1, 1.3], opacity: [0.4, 0] }} transition={{ duration: 2, repeat: Infinity, ease: 'easeOut', delay: 0.5 }} style={{ position: 'absolute', width: '40px', height: '40px', borderRadius: '50%', border: '2px solid rgba(255,255,255,0.6)' }} />
      <motion.div animate={{ rotate: 360 }} transition={{ duration: 3, repeat: Infinity, ease: 'linear' }} style={{ position: 'absolute', width: '32px', height: '32px', borderRadius: '50%', border: '2px solid transparent', borderTopColor: 'rgba(255,255,255,0.9)', borderRightColor: 'rgba(255,255,255,0.4)' }} />
      <div style={{ position: 'absolute', color: '#fff', fontWeight: 800, fontSize: '14px', textShadow: '0 0-8px rgba(255,255,255,0.8)', userSelect: 'none' }}>{num}</div>
    </div>
  );
  const R2RKeyMetricsAnimation = ({ metrics }) => {
    const colors = ['#00828A', '#00A3AD', '#007077', '#005F66', '#004D52', '#003F44'];
    const subtitles = [
      'Average days to post entries',
      'Percent of intercompany trades matched',
      'Days from period close to finalized reports',
      'Percent of manual entries requiring correction',
      'Percent of system-automated entries posted correctly',
      'Total count of adjustments made after period close'
    ];

    const icons = [
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10" /><polyline points="12 6 12 12 16 14" /></svg>,
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14" /><polyline points="22 4 12 14.01 9 11.01" /></svg>,
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><polyline points="23 6 13.5 15.5 8.5 10.5 1 18" /><polyline points="17 6 23 6 23 12" /></svg>,
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z" /><line x1="12" y1="9" x2="12" y2="13" /><line x1="12" y1="17" x2="12.01" y2="17" /></svg>,
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><rect x="2" y="2" width="20" height="8" rx="2" ry="2" /><rect x="2" y="14" width="20" height="8" rx="2" ry="2" /><line x1="6" y1="6" x2="6.01" y2="6" /><line x1="6" y1="18" x2="6.01" y2="18" /></svg>,
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3" /><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" /><path d="M3 12c0 1.66 4 3 9 3s9-1.34 9-3" /></svg>
    ];

    return (
      <div style={{ display: 'flex', flexDirection: 'row', alignItems: 'center', gap: '12px', width: '100%', height: '302px' }}>
        <KpiWheel colors={colors} label="R2R" height={302} width={140} />
        <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: '6px', height: '100%', justifyContent: 'center' }}>
          {metrics.map((m, i) => {
            const color = colors[i % colors.length];
            return (
              <motion.div
                key={i}
                initial={{ opacity: 0, x: 15 }}
                animate={{ opacity: 1, x: 0 }}
                whileHover={{ scale: 1.015, x: 2 }}
                transition={{ delay: i * 0.04 }}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '10px',
                  height: '44px',
                  padding: '0 12px',
                  background: 'rgba(255, 255, 255, 0.65)',
                  backdropFilter: 'blur(8px)',
                  borderRadius: '22px',
                  border: '1px solid rgba(226, 232, 240, 0.8)',
                  borderLeft: `4px solid ${color}`,
                  boxShadow: '0 2px 6px rgba(0,0,0,0.02)',
                  width: '100%',
                  overflow: 'hidden'
                }}
              >
                <div style={{
                  width: 24, height: 24, borderRadius: '50%',
                  border: `1.5px solid ${color}`,
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                  padding: '2px', flexShrink: 0
                }}>
                  <div style={{
                    width: '100%', height: '100%', borderRadius: '50%',
                    background: color, display: 'flex', alignItems: 'center',
                    justifyContent: 'center', color: '#fff', fontSize: '11px', fontWeight: 800
                  }}>
                    {i + 1}
                  </div>
                </div>
                <div style={{ width: 14, height: 14, color: color, display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0 }}>
                  {icons[i]}
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', minWidth: 0, flex: 1 }}>
                  <div style={{ fontSize: '11px', fontWeight: 700, color: '#1e293b', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', textAlign: 'left' }}>
                    {m}
                  </div>
                  <div style={{ fontSize: '9px', fontWeight: 500, color: '#64748b', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', textAlign: 'left', marginTop: '1px' }}>
                    {subtitles[i]}
                  </div>
                </div>
              </motion.div>
            );
          })}
        </div>
      </div>
    );
  };

  const FaqModal = () => (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      style={{
        position: 'fixed', inset: 0, zIndex: 10000,
        background: 'rgba(15, 23, 42, 0.85)', backdropFilter: 'blur(12px)',
        display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '24px'
      }}
      onClick={() => setShowFaqModal(false)}
    >
      <motion.div
        initial={{ scale: 0.9, opacity: 0, y: 20 }}
        animate={{ scale: 1, opacity: 1, y: 0 }}
        exit={{ scale: 0.9, opacity: 0, y: 20 }}
        style={{
          background: '#fff', width: '100%', maxWidth: '800px', maxHeight: '85vh',
          borderRadius: '24px', position: 'relative', overflow: 'hidden',
          display: 'flex', flexDirection: 'column', boxShadow: '0 25px 50px -12px rgba(0, 0, 0, 0.5)'
        }}
        onClick={e => e.stopPropagation()}
      >
        <div style={{
          padding: '32px 40px 24px', borderBottom: '1px solid #f1f5f9',
          display: 'flex', justifyContent: 'space-between', alignItems: 'center',
          background: 'linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%)'
        }}>
          <div>
            <h2 style={{ margin: 0, fontSize: '24px', fontWeight: 800, color: '#1e293b' }}>Frequently Asked Questions</h2>
            <p style={{ margin: '4px 0 0', fontSize: '14px', color: '#64748b' }}>Learn more about R2R Process Mining</p>
          </div>
          <button
            onClick={() => setShowFaqModal(false)}
            style={{
              background: '#f1f5f9', border: 'none', width: '40px', height: '40px',
              borderRadius: '50%', cursor: 'pointer', display: 'flex', alignItems: 'center',
              justifyContent: 'center', color: '#64748b', fontSize: '20px', transition: 'all 0.2s'
            }}
            onMouseOver={e => e.currentTarget.style.background = '#e2e8f0'}
            onMouseOut={e => e.currentTarget.style.background = '#f1f5f9'}
          >
            ✕
          </button>
        </div>
        <div style={{ padding: '24px 40px 40px', overflowY: 'auto', flex: 1 }}>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
            {R2R_FAQS.map((faq, i) => (
              <FaqItem key={i} q={faq.q} a={faq.a} bullets={faq.bullets} accentColor={C.teal} />
            ))}
          </div>
        </div>
      </motion.div>
    </motion.div>
  );

  const R2RProcessTreeFlow = ({ steps }) => {
    const colors = [C.teal, '#00A3AD', '#007077', '#005F66', '#004D52', '#003F44'];
    return (
      <div style={{ position: 'relative', width: '100%', display: 'flex', flexDirection: 'column', gap: '10px', padding: '10px 0' }}>
        {steps.map((step, i) => {
          return (
            <motion.div key={i} initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: i * 0.06 }} style={{ display: 'flex', alignItems: 'center', gap: '12px', position: 'relative' }}>
              <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', position: 'relative', width: '32px', height: '32px', flexShrink: 0 }}>
                <div style={{ width: '32px', height: '32px', background: '#fff', border: `3px solid ${colors[i % colors.length]}`, borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center', fontWeight: 900, fontSize: '12px', color: colors[i % colors.length], zIndex: 2, boxShadow: `0 0 10px ${colors[i % colors.length]}22` }}>
                  {i + 1}
                </div>
                {i < steps.length - 1 && (
                  <div style={{ position: 'absolute', top: '32px', bottom: '-14px', width: '3px', background: `linear-gradient(to bottom, ${colors[i % colors.length]}, ${colors[(i + 1) % colors.length]})`, zIndex: 1 }} />
                )}
              </div>
              <div style={{ flex: 1, background: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: '10px', padding: '10px 14px', display: 'flex', flexDirection: 'column', justifyContent: 'center' }}>
                <div style={{ color: colors[i % colors.length], fontWeight: 800, fontSize: '9px', textTransform: 'uppercase', letterSpacing: '0.8px', marginBottom: '2px' }}>
                  Step {i + 1}
                </div>
                <div style={{ fontSize: '12px', color: '#1e293b', fontWeight: 600, lineHeight: 1.4 }}>{step}</div>
              </div>
            </motion.div>
          );
        })}
      </div>
    );
  };

  return (
    <div ref={screenshotRef} style={{
      fontFamily: "'Segoe UI', -apple-system, sans-serif",
      background: C.bg, height: '100vh', display: 'flex', flexDirection: 'column'
    }}>
      <style>{`
        @keyframes spin{to{transform:rotate(360deg)}}
        *{box-sizing:border-box}
        ::-webkit-scrollbar{width:5px;height:5px}
        ::-webkit-scrollbar-track{background:transparent}
        ::-webkit-scrollbar-thumb{background:#D2D0CE;border-radius:3px}
        ::-webkit-scrollbar-thumb:hover{background:#A19F9D}
      `}</style>

      <LoadingOverlay visible={loading} progress={loadProg} label={loadLabel} />

      <AnimatePresence>
        {showAvailablePopup && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            style={{
              position: 'fixed',
              inset: 0,
              zIndex: 999999,
              background: 'rgba(15, 23, 42, 0.45)',
              backdropFilter: 'blur(16px)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              padding: '20px'
            }}
          >
            <motion.div
              initial={{ scale: 0.9, y: 20, opacity: 0 }}
              animate={{ scale: 1, y: 0, opacity: 1 }}
              exit={{ scale: 0.9, y: 20, opacity: 0 }}
              transition={{ type: 'spring', damping: 25, stiffness: 300 }}
              style={{
                background: 'rgba(30, 41, 59, 0.85)',
                border: '1px solid rgba(255, 255, 255, 0.15)',
                borderRadius: '16px',
                padding: '2.5rem',
                width: '100%',
                maxWidth: '460px',
                color: '#fff',
                boxShadow: '0 25px 50px -12px rgba(0, 0, 0, 0.5)',
                backdropFilter: 'blur(20px)',
                textAlign: 'center'
              }}
            >
              <div style={{
                width: '64px',
                height: '64px',
                background: `${C.teal}22`,
                border: `1.5px solid ${C.teal}`,
                borderRadius: '50%',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                margin: '0 auto 1.5rem',
                fontSize: '28px'
              }}>
                🔒
              </div>
              <h3 style={{ fontSize: '1.4rem', fontWeight: 800, margin: '0 0 1rem', letterSpacing: '-0.3px', color: '#fff' }}>
                Uploading Restricted
              </h3>
              <p style={{ fontSize: '14px', color: '#cbd5e1', lineHeight: '1.5', margin: '0 0 1.5rem' }}>
                Uploading is Restricted in the current environment. Kindly contact administrator to get access.
              </p>
              <button
                onClick={() => {
                  window.open('https://ajalabs.ai/', '_blank', 'noopener,noreferrer');
                  setShowAvailablePopup(false);
                }}
                style={{
                  background: C.teal,
                  color: '#fff',
                  border: 'none',
                  padding: '12px 30px',
                  borderRadius: '8px',
                  fontWeight: 700,
                  fontSize: '14px',
                  cursor: 'pointer',
                  width: '100%',
                  boxShadow: `0 4px 12px rgba(0,130,138,0.3)`,
                  transition: 'all 0.2s'
                }}
                onMouseOver={e => { e.currentTarget.style.filter = 'brightness(1.1)'; e.currentTarget.style.transform = 'translateY(-1px)'; }}
                onMouseOut={e => { e.currentTarget.style.filter = 'none'; e.currentTarget.style.transform = 'none'; }}
              >
                Contact Admin
              </button>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Header */}
      <div style={{
        background: C.headerBg, padding: '10px 20px', flexShrink: 0,
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        boxShadow: '0 2px 8px rgba(0,0,0,.2)'
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <img
            src="/logo.png"
            alt="AJALabs Logo"
            onClick={onBackHome}
            title="Back to Home"
            style={{ height: '36px', objectFit: 'contain', cursor: 'pointer', borderRadius: 4 }}
          />
          <div>
            <div style={{ fontWeight: 700, fontSize: 16, color: '#fff' }}>Record to Report Process Explorer</div>
            <div style={{ fontSize: 10, color: 'rgba(255,255,255,.5)' }}>R2R Process Mining Dashboard</div>
          </div>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <div style={{ fontSize: 12, color: 'rgba(255,255,255,0.7)' }}>
            User: <strong style={{ color: '#fff' }}>{currentUser}</strong>
          </div>
          {step === 'dashboard' && (
            <button
              id="r2r-screenshot-btn"
              onClick={handleScreenshot}
              title="Download full report screenshot"
              style={{
                width: 34, height: 34, borderRadius: '50%',
                background: screenshotting ? 'rgba(0,130,138,0.5)' : 'linear-gradient(135deg, rgba(0,130,138,0.9) 0%, rgba(0,80,85,0.95) 100%)',
                border: '1.5px solid rgba(255,255,255,0.25)',
                boxShadow: screenshotting ? '0 0 0 3px rgba(0,130,138,0.4)' : '0 2px 12px rgba(0,130,138,0.45), 0 0 0 1px rgba(255,255,255,0.08)',
                cursor: screenshotting ? 'not-allowed' : 'pointer',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                flexShrink: 0, transition: 'all 0.22s ease',
              }}
              onMouseOver={e => { if (!screenshotting) { e.currentTarget.style.transform = 'scale(1.12)'; } }}
              onMouseOut={e => { e.currentTarget.style.transform = 'scale(1)'; }}
            >
              {screenshotting ? (
                <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" style={{ animation: 'spin 1s linear infinite' }}><path d="M21 12a9 9 0 11-6.219-8.56" /></svg>
              ) : (
                <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M23 19a2 2 0 01-2 2H3a2 2 0 01-2-2V8a2 2 0 012-2h4l2-3h6l2 3h4a2 2 0 012 2z" />
                  <circle cx="12" cy="13" r="4" />
                </svg>
              )}
            </button>
          )}
          <button onClick={onSignOut} style={{
            background: 'rgba(209, 52, 56, 0.85)', color: '#fff', border: 'none',
            padding: '6px 12px', borderRadius: 4, cursor: 'pointer', fontSize: 11, fontWeight: 700,
            transition: 'all 0.2s'
          }}
            onMouseOver={e => e.currentTarget.style.background = '#D13438'}
            onMouseOut={e => e.currentTarget.style.background = 'rgba(209, 52, 56, 0.85)'}
          >
            Sign Out
          </button>
        </div>
      </div>

      {/* Step Info: Premium Intro screen */}
      {step === 'info' && (
        <div style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', overflowY: 'auto', position: 'relative' }}>
          <AnimatePresence>
            {showFaqModal && <FaqModal />}
          </AnimatePresence>
          <div className="process-ribbon-container">
            <div className="process-ribbon-content">
              {[...r2rShortSteps, ...r2rShortSteps, ...r2rShortSteps, ...r2rShortSteps].map((s, i) => {
                const isFirst = i % r2rShortSteps.length === 0;
                return (
                  <React.Fragment key={i}>
                    <div
                      className="process-ribbon-item"
                      style={isFirst ? { background: C.headerBg, color: '#fff', borderColor: C.headerBg, boxShadow: `0 4px 12px ${C.headerBg}33` } : {}}
                    >
                      <span style={{ display: 'inline-flex', alignItems: 'center', color: isFirst ? '#fff' : 'inherit' }}>
                        {s.icon}
                      </span>
                      <span style={{ marginLeft: 4 }}>{s.text}</span>
                    </div>
                    {i < r2rShortSteps.length * 4 - 1 && <div className="process-ribbon-arrow">→</div>}
                  </React.Fragment>
                );
              })}
            </div>
          </div>
          <div style={{ maxWidth: 1100, width: '100%', display: 'flex', flexDirection: 'column', gap: 16, padding: '20px 24px 30px' }}>
            <div style={{ borderBottom: '2px solid #E2E8F0', paddingBottom: 12, display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
              <div style={{ flex: 1 }}>
                <div style={{ fontSize: 11, fontWeight: 700, color: C.teal, textTransform: 'uppercase', letterSpacing: 1, marginBottom: 4 }}>Process Overview</div>
                <h1 style={{ margin: '0 0 6px', fontSize: 22, fontWeight: 700, color: '#1e293b' }}>Record to Report (R2R)</h1>
                <p style={{ margin: 0, fontSize: 13, color: '#475569', lineHeight: 1.6, maxWidth: 900 }}>The Record-to-Report process spans the full financial accounting lifecycle — from journal entry creation and approval, posting to the general ledger, intercompany reconciliation, through trial balance generation and final financial reporting. Process mining on R2R data helps uncover posting delays, audit trail gaps, reconciliation failures, and bottlenecks.</p>
              </div>
              <button
                onClick={() => setShowFaqModal(true)}
                style={{
                  background: `linear-gradient(135deg, ${C.teal} 0%, #00A3AD 100%)`,
                  color: '#fff', border: 'none', padding: '10px 20px', borderRadius: '12px',
                  fontSize: '13px', fontWeight: 700, cursor: 'pointer', display: 'flex',
                  alignItems: 'center', gap: '8px', boxShadow: `0 4px 12px rgba(0, 130, 138, 0.2)`,
                  transition: 'all 0.2s', flexShrink: 0, marginTop: '10px'
                }}
                onMouseOver={e => e.currentTarget.style.transform = 'translateY(-2px)'}
                onMouseOut={e => e.currentTarget.style.transform = 'translateY(0)'}
              >
                <span>Read FAQ</span><span style={{ fontSize: '16px' }}>💬</span>
              </button>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: '1.3fr 1fr', gap: 24, alignItems: 'stretch' }}>
              {/* Left Column: Process Steps */}
              <div style={{ background: '#fff', border: '1px solid #E2E8F0', borderRadius: 16, padding: '16px 20px', boxShadow: '0 4px 12px rgba(0,0,0,0.04)' }}>
                <div style={{ fontSize: 11, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: 0.8, marginBottom: 12 }}>Process Steps</div>
                <R2RProcessTreeFlow steps={r2rSteps} />
              </div>

              {/* Right Column: Key Metrics */}
              <div style={{ background: '#fff', border: '1px solid #E2E8F0', borderRadius: 16, padding: '16px 20px', boxShadow: '0 4px 12px rgba(0,0,0,0.04)', display: 'flex', flexDirection: 'column' }}>
                <div style={{ fontSize: 11, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: 0.8, marginBottom: 12 }}>Key Metrics</div>
                <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                  <R2RKeyMetricsAnimation metrics={r2rKpis} />
                </div>
              </div>
            </div>
            <div style={{ display: 'flex', justifyContent: 'flex-end', paddingTop: 4 }}>
              <button onClick={() => setStep('choose')} style={{ background: C.teal, color: '#fff', border: 'none', padding: '10px 28px', borderRadius: 8, fontSize: 13, fontWeight: 700, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 8, boxShadow: `0 4px 12px rgba(0,130,138,0.25)`, transition: 'all 0.2s' }} onMouseOver={e => { e.currentTarget.style.background = C.tealDark; e.currentTarget.style.boxShadow = `0 6px 16px rgba(0,130,138,0.35)`; }} onMouseOut={e => { e.currentTarget.style.background = C.teal; e.currentTarget.style.boxShadow = `0 4px 12px rgba(0,130,138,0.25)`; }}>
                Continue <span style={{ fontSize: 14 }}>→</span>
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Step Choose: Choose how to load your data */}
      {step === 'choose' && (
        <div style={{
          flex: 1,
          overflowY: 'auto',
          display: 'flex',
          flexDirection: 'column',
          justifyContent: 'center',
          alignItems: 'center',
          padding: '2rem',
          background: 'radial-gradient(circle at top left, rgba(0,130,138,0.08), transparent 60%)',
          position: 'relative'
        }}>
          {/* Snapshot Button */}
          <button
            onClick={() => window.open('/snapshot r2r.pdf', '_blank')}
            style={{
              position: 'absolute',
              top: '24px',
              right: '24px',
              background: C.teal,
              color: '#fff',
              border: 'none',
              padding: '9px 18px',
              borderRadius: '8px',
              fontWeight: 700,
              fontSize: '12px',
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
              boxShadow: '0 4px 12px rgba(0,130,138,0.3)',
              transition: 'all 0.2s'
            }}
            onMouseOver={e => { e.currentTarget.style.filter = 'brightness(1.1)'; e.currentTarget.style.transform = 'translateY(-1px)'; }}
            onMouseOut={e => { e.currentTarget.style.filter = 'none'; e.currentTarget.style.transform = 'none'; }}
          >
            📸 Snapshot
          </button>

          <motion.div
            initial={{ opacity: 0, scale: 0.98 }}
            animate={{ opacity: 1, scale: 1 }}
            transition={{ duration: 0.4 }}
            style={{ textAlign: 'center', marginBottom: '2.5rem' }}
          >
            <h2 style={{ fontSize: '2.2rem', fontWeight: 800, color: '#1B2A4A', margin: '0 0 0.5rem' }}>
              Choose Data Source
            </h2>
            <p style={{ fontSize: '15px', color: '#605E5C', margin: 0 }}>
              Select how you want to ingest event logs into the Record to Report model
            </p>
          </motion.div>

          <div style={{
            display: 'flex',
            width: '100%',
            height: 320,
            gap: 16,
            maxWidth: '700px',
            alignItems: 'stretch'
          }}>
            {/* Card 1: Build Event Log */}
            <motion.div
              layout
              whileHover={{ y: -4, boxShadow: '0 12px 30px rgba(0,0,0,0.08)' }}
              style={{
                flex: hoveredSide === 'build' ? 1.7 : (hoveredSide === 'csv' ? 0.6 : 1),
                background: 'rgba(255,255,255,0.7)',
                backdropFilter: 'blur(20px)',
                border: '2px solid rgba(255, 255, 255, 0.4)',
                borderRadius: '12px',
                padding: '28px 24px',
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'center',
                gap: 14,
                textAlign: 'center',
                cursor: 'pointer',
                overflow: 'hidden'
              }}
              onClick={() => setShowAvailablePopup(true)}
              onMouseEnter={() => setHoveredSide('build')}
              onMouseLeave={() => setHoveredSide(null)}
              animate={{ borderColor: hoveredSide === 'build' ? C.teal : 'rgba(255, 255, 255, 0.4)' }}
            >
              <motion.div layout style={{
                width: '56px',
                height: '56px',
                background: `${C.teal}10`,
                borderRadius: '10px',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                border: `1.5px solid ${C.teal}30`,
                flexShrink: 0
              }}>
                <svg width="26" height="26" viewBox="0 0 24 24" fill="none" stroke={C.teal} strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round">
                  <rect x="2" y="3" width="6" height="6" rx="1" />
                  <rect x="16" y="3" width="6" height="6" rx="1" />
                  <rect x="16" y="15" width="6" height="6" rx="1" />
                  <rect x="2" y="15" width="6" height="6" rx="1" />
                  <path d="M8 6h8M19 9v6M16 18H8M5 15V9" />
                </svg>
              </motion.div>
              <motion.div layout style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
                <motion.div layout style={{ fontSize: '1.15rem', fontWeight: 700, color: '#1B2A4A', marginBottom: '0.75rem', whiteSpace: hoveredSide === 'csv' ? 'nowrap' : 'normal' }}>
                  Build Event Log
                </motion.div>
                <AnimatePresence>
                  {hoveredSide !== 'csv' && (
                    <motion.p
                      initial={{ opacity: 0 }}
                      animate={{ opacity: 1 }}
                      exit={{ opacity: 0 }}
                      style={{ fontSize: '12.5px', color: '#605E5C', lineHeight: '1.5', margin: 0 }}
                    >
                      Upload raw ERP tables  and let the system
                      automatically build the process event log.
                    </motion.p>
                  )}
                </AnimatePresence>
              </motion.div>
              <motion.button
                layout
                onClick={(e) => { e.stopPropagation(); setShowAvailablePopup(true); }}
                style={{
                  background: C.teal,
                  color: '#fff',
                  border: 'none',
                  padding: '11px 28px',
                  borderRadius: '7px',
                  fontWeight: 700,
                  fontSize: '13px',
                  cursor: 'pointer',
                  width: '100%',
                  marginTop: 'auto'
                }}
                onMouseOver={e => e.currentTarget.style.background = C.tealDark}
                onMouseOut={e => e.currentTarget.style.background = C.teal}
              >
                {hoveredSide === 'csv' ? 'Build' : 'Build Event Log'}
              </motion.button>
            </motion.div>

            {/* Card 2: Upload Pre-built CSV or Excel */}
            <motion.div
              layout
              whileHover={{ y: -4, boxShadow: '0 12px 30px rgba(0,0,0,0.08)' }}
              style={{
                flex: hoveredSide === 'csv' ? 1.7 : (hoveredSide === 'build' ? 0.6 : 1),
                background: 'rgba(255,255,255,0.7)',
                backdropFilter: 'blur(20px)',
                border: '2px solid rgba(255, 255, 255, 0.4)',
                borderRadius: '12px',
                padding: '28px 24px',
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'center',
                gap: 14,
                textAlign: 'center',
                cursor: 'pointer',
                overflow: 'hidden'
              }}
              onClick={() => setShowAvailablePopup(true)}
              onMouseEnter={() => setHoveredSide('csv')}
              onMouseLeave={() => setHoveredSide(null)}
              animate={{ borderColor: hoveredSide === 'csv' ? C.teal : 'rgba(255, 255, 255, 0.4)' }}
            >
              <motion.div layout style={{
                width: '56px',
                height: '56px',
                background: `${C.teal}10`,
                borderRadius: '10px',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                border: `1.5px solid ${C.teal}30`,
                flexShrink: 0
              }}>
                <svg width="26" height="26" viewBox="0 0 24 24" fill="none" stroke={C.teal} strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z" />
                  <polyline points="14 2 14 8 20 8" />
                  <path d="M8 13h8M8 17h8" />
                </svg>
              </motion.div>
              <motion.div layout style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
                <motion.div layout style={{ fontSize: '1.15rem', fontWeight: 700, color: '#1B2A4A', marginBottom: '0.75rem', whiteSpace: hoveredSide === 'build' ? 'nowrap' : 'normal' }}>
                  Upload Pre-built CSV or Excel
                </motion.div>
                <AnimatePresence>
                  {hoveredSide !== 'build' && (
                    <motion.p
                      initial={{ opacity: 0 }}
                      animate={{ opacity: 1 }}
                      exit={{ opacity: 0 }}
                      style={{ fontSize: '12.5px', color: '#605E5C', lineHeight: '1.5', margin: 0 }}
                    >
                      Already have a formatted event log? Upload your pre-built wide-format CSV or Excel directly to launch the dashboard.                    </motion.p>
                  )}
                </AnimatePresence>
              </motion.div>
              <motion.button
                layout
                onClick={(e) => { e.stopPropagation(); setShowAvailablePopup(true); }}
                style={{
                  background: C.teal,
                  color: '#fff',
                  border: 'none',
                  padding: '11px 28px',
                  borderRadius: '7px',
                  fontWeight: 700,
                  fontSize: '13px',
                  cursor: 'pointer',
                  width: '100%',
                  marginTop: 'auto'
                }}
                onMouseOver={e => e.currentTarget.style.background = C.tealDark}
                onMouseOut={e => e.currentTarget.style.background = C.teal}
              >
                {hoveredSide === 'build' ? 'Upload' : 'Upload CSV or Excel'}
              </motion.button>
            </motion.div>
          </div>

        </div>
      )}

      {/* Body Content */}
      {step === 'dashboard' && (
        <div style={{ flex: 1, overflowY: 'auto', padding: '12px 14px 40px', display: 'flex', flexDirection: 'column', gap: 10 }}>
          {/* Filters */}
          <div style={{
            background: C.card, borderRadius: 8, padding: '10px 14px',
            border: `1px solid ${C.border}`, boxShadow: '0 2px 6px rgba(0,0,0,.04)'
          }}>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 12, alignItems: 'end' }}>
              <FilterSelect label="Case ID" value={selected.case_id} options={slicerOptions.case_id} onChange={val => setSelected(prev => ({ ...prev, case_id: val }))} />
              <FilterSelect label="User" value={selected.user} options={slicerOptions.user} onChange={val => setSelected(prev => ({ ...prev, user: val }))} />
              <FilterSelect label="Status" value={selected.status} options={slicerOptions.status} onChange={val => setSelected(prev => ({ ...prev, status: val }))} />
              <FilterSelect label="Month" value={selected.month} options={slicerOptions.month} onChange={val => setSelected(prev => ({ ...prev, month: val }))} />
              <div style={{ display: 'flex', gap: 8, height: '32px' }}>
                <button onClick={resetAll} style={{
                  padding: '6px 16px', fontSize: 12, fontWeight: 700,
                  background: '#F3F2F1', color: '#323130', border: `1px solid #D2D0CE`,
                  borderRadius: 4, cursor: 'pointer'
                }}>Reset</button>
              </div>
            </div>
          </div>

          {/* KPIs */}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 8 }}>
            <div style={{ background: C.card, borderRadius: 6, padding: '12px', borderLeft: `4px solid ${C.teal}`, boxShadow: '0 2px 6px rgba(0,0,0,.05)' }}>
              <div style={{ fontSize: 10, fontWeight: 600, color: C.teal, textTransform: 'uppercase', marginBottom: 2 }}>Total Cases</div>
              <div style={{ fontSize: 20, fontWeight: 700, color: '#000' }}>{kpis.cases}</div>
            </div>
            <div style={{ background: C.card, borderRadius: 6, padding: '12px', borderLeft: `4px solid ${C.green}`, boxShadow: '0 2px 6px rgba(0,0,0,.05)' }}>
              <div style={{ fontSize: 10, fontWeight: 600, color: C.green, textTransform: 'uppercase', marginBottom: 2 }}>Avg Cycle Time</div>
              <div style={{ fontSize: 20, fontWeight: 700, color: '#000' }}>{kpis.cycle}</div>
            </div>
            <div style={{ background: C.card, borderRadius: 6, padding: '12px', borderLeft: `4px solid ${C.red}`, boxShadow: '0 2px 6px rgba(0,0,0,.05)' }}>
              <div style={{ fontSize: 10, fontWeight: 600, color: C.red, textTransform: 'uppercase', marginBottom: 2 }}>Rejection/Rework Rate</div>
              <div style={{ fontSize: 20, fontWeight: 700, color: '#000' }}>{kpis.rework}</div>
            </div>
            <div style={{ background: C.card, borderRadius: 6, padding: '12px', borderLeft: `4px solid ${C.orange}`, boxShadow: '0 2px 6px rgba(0,0,0,.05)' }}>
              <div style={{ fontSize: 10, fontWeight: 600, color: C.orange, textTransform: 'uppercase', marginBottom: 2 }}>Manual Correction Rate</div>
              <div style={{ fontSize: 20, fontWeight: 700, color: '#000' }}>{kpis.manual}</div>
            </div>
          </div>

          {/* Process Map + Charts */}
          <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 10 }}>
            {/* ReactFlow Process Map */}
            <div style={{
              background: C.card, borderRadius: 8, border: `1px solid ${C.border}`,
              display: 'flex', flexDirection: 'column', height: 600, overflow: 'hidden'
            }}>
              <div style={{
                padding: '12px 14px', borderBottom: `1px solid ${C.border}`,
                background: C.teal, display: 'flex', justifyContent: 'space-between', alignItems: 'center'
              }}>
                <div>
                  <div style={{ fontSize: 13, fontWeight: 700, color: '#fff' }}>Process Flow Map</div>
                  <div style={{ fontSize: 10, color: 'rgba(255,255,255,0.7)' }}>Record to Report Sequence</div>
                </div>
                <div style={{ display: 'flex', gap: 4, background: 'rgba(255,255,255,0.2)', padding: 2, borderRadius: 4 }}>
                  <button onClick={() => setLayoutDir('LR')} style={{
                    fontSize: 11, padding: '4px 8px', border: 'none', cursor: 'pointer', borderRadius: 3,
                    background: layoutDir === 'LR' ? '#fff' : 'transparent',
                    color: layoutDir === 'LR' ? C.teal : '#fff', fontWeight: 700
                  }}>Horizontal</button>
                  <button onClick={() => setLayoutDir('TB')} style={{
                    fontSize: 11, padding: '4px 8px', border: 'none', cursor: 'pointer', borderRadius: 3,
                    background: layoutDir === 'TB' ? '#fff' : 'transparent',
                    color: layoutDir === 'TB' ? C.teal : '#fff', fontWeight: 700
                  }}>Vertical</button>
                </div>
              </div>
              <div style={{ flex: 1, position: 'relative', background: '#FAFAFA' }}>
                <ReactFlow
                  nodes={rfNodes} edges={rfEdges}
                  nodeTypes={nodeTypes}
                  fitView fitViewOptions={{ padding: 0.2 }}
                  minZoom={0.2} maxZoom={2}
                  proOptions={{ hideAttribution: true }}
                >
                  <Background color="#ccc" gap={20} size={1.2} />
                  <Controls />
                  <MiniMap />
                </ReactFlow>
              </div>
            </div>

            {/* Core Analytics Charts */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: 10, height: 600 }}>
              <ChartCard title="Happy Path vs Deviations" loading={chartsLoading} skeletonType="pie" style={{ flex: 1 }}>
                <ResponsiveContainer width="100%" height={180}>
                  <PieChart>
                    <Pie data={donutData} dataKey="value" cx="50%" cy="50%" innerRadius={45} outerRadius={65} paddingAngle={3}>
                      {donutData.map((entry, index) => (
                        <Cell key={index} fill={entry.color} />
                      ))}
                    </Pie>
                    <Legend verticalAlign="bottom" height={24} iconSize={10} style={{ fontSize: 11 }} />
                    <ReChartsTooltip formatter={(v) => [`${v} cases`, 'Volume']} />
                  </PieChart>
                </ResponsiveContainer>
              </ChartCard>

              <ChartCard title="Activity Frequency" loading={chartsLoading} skeletonType="bar-horizontal" style={{ flex: 1.2 }}>
                <ResponsiveContainer width="100%" height={200}>
                  <BarChart data={barChartData} layout="vertical" margin={{ left: 10, right: 10, top: 5, bottom: 5 }}>
                    <XAxis type="number" stroke="#888" fontSize={9} />
                    <YAxis dataKey="name" type="category" stroke="#888" fontSize={8} width={110} tickFormatter={(tick) => tick.length > 22 ? `${tick.substring(0, 20)}...` : tick} />
                    <ReChartsTooltip formatter={(v) => [`${v} cases`, 'Occurrences']} />
                    <Bar dataKey="count" fill={C.teal} radius={[0, 4, 4, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </ChartCard>
            </div>
          </div>

          {/* Postings Volume + Case Table */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
            <ChartCard title="Monthly Posting Volume" loading={chartsLoading} skeletonType="line">
              <ResponsiveContainer width="100%" height={260}>
                <LineChart data={monthlyData} margin={{ left: 5, right: 20, top: 10, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
                  <XAxis dataKey="month" stroke="#888" fontSize={10} />
                  <YAxis stroke="#888" fontSize={10} />
                  <ReChartsTooltip />
                  <Line type="monotone" dataKey="volume" stroke={C.teal} strokeWidth={3} activeDot={{ r: 8 }} />
                </LineChart>
              </ResponsiveContainer>
            </ChartCard>

            <ChartCard title="Case Details & Drill-down" loading={chartsLoading} skeletonType="timeline">
              {selectedCaseId === 'ALL' ? (
                <div style={{ overflowY: 'auto', height: 260 }}>
                  <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px' }}>
                    <thead style={{ position: 'sticky', top: 0, background: '#F0F2F5', zIndex: 1 }}>
                      <tr style={{ borderBottom: `1px solid ${C.border}` }}>
                        <th style={{ padding: '8px', textAlign: 'left', color: C.slate }}>Case ID</th>
                        <th style={{ padding: '8px', textAlign: 'left', color: C.slate }}>Status</th>
                        <th style={{ padding: '8px', textAlign: 'left', color: C.slate }}>Creator</th>
                      </tr>
                    </thead>
                    <tbody>
                      {Object.keys(casesMap).map((cid) => {
                        const firstEv = casesMap[cid][0];
                        const isHappy = !casesMap[cid].map(e => e.activity).includes('Rejected & Reworked') && !casesMap[cid].map(e => e.activity).includes('Manual Correction');
                        return (
                          <tr key={cid} onClick={() => setSelectedCaseId(cid)} style={{
                            borderBottom: `1px solid ${C.border}`, cursor: 'pointer',
                            background: '#fff'
                          }}
                            onMouseEnter={e => e.currentTarget.style.background = '#F3F2F1'}
                            onMouseLeave={e => e.currentTarget.style.background = '#fff'}
                          >
                            <td style={{ padding: '8px', fontWeight: 600, color: C.teal }}>{cid}</td>
                            <td style={{ padding: '8px', color: isHappy ? C.green : C.red }}>{isHappy ? 'Happy Path' : 'Deviation'}</td>
                            <td style={{ padding: '8px', color: C.slate }}>{firstEv.user}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div style={{ display: 'flex', flexDirection: 'column', height: 260 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8, paddingBottom: 6, borderBottom: `1px solid ${C.border}` }}>
                    <div><span style={{ fontSize: 11, color: C.slate }}>Log for Case: </span><strong style={{ fontSize: 13, color: '#323130' }}>{selectedCaseId}</strong></div>
                    <button onClick={() => setSelectedCaseId('ALL')} style={{ fontSize: 10, background: C.teal, color: '#fff', border: 'none', padding: '4px 8px', borderRadius: 4, cursor: 'pointer', fontWeight: 600 }}>Back to List</button>
                  </div>
                  <div style={{ overflowY: 'auto', flex: 1 }}>
                    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '11px' }}>
                      <thead style={{ position: 'sticky', top: 0, background: '#F0F2F5', zIndex: 1 }}>
                        <tr>
                          <th style={{ padding: '6px', textAlign: 'left', color: C.slate }}>Activity</th>
                          <th style={{ padding: '6px', textAlign: 'left', color: C.slate }}>Timestamp</th>
                          <th style={{ padding: '6px', textAlign: 'left', color: C.slate }}>User</th>
                        </tr>
                      </thead>
                      <tbody>
                        {activeCaseEvents.map((e, i) => (
                          <tr key={i} style={{ borderBottom: `1px solid ${C.border}`, background: i % 2 === 0 ? '#fff' : '#fafafa' }}>
                            <td style={{ padding: '6px', color: '#323130', fontWeight: 600 }}>{e.activity}</td>
                            <td style={{ padding: '6px', color: '#605E5C' }}>{e.timestamp}</td>
                            <td style={{ padding: '6px', color: '#605E5C' }}>{e.user}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </ChartCard>
          </div>
        </div>
      )}

      {screenshotting && (
        <div
          id="screenshot-loading-overlay"
          className="no-screenshot"
          style={{
            position: 'fixed',
            inset: 0,
            zIndex: 99999,
            background: 'rgba(15, 23, 42, 0.75)',
            backdropFilter: 'blur(4px)',
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
            gap: 16
          }}
        >
          <div style={{
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            background: 'rgba(30, 41, 59, 0.9)',
            padding: '30px 50px',
            borderRadius: '12px',
            border: '1px solid rgba(255, 255, 255, 0.1)',
            boxShadow: '0 20px 25px -5px rgba(0,0,0,0.5)',
            color: '#fff',
            gap: 12
          }}>
            <div style={{
              width: 36, height: 36,
              border: '3px solid rgba(255,255,255,0.2)',
              borderTop: `3px solid ${C.teal}`,
              borderRadius: '50%',
              animation: 'spin 1s linear infinite'
            }} />
            <div style={{ fontSize: 16, fontWeight: 700 }}>Generating Screenshot...</div>
            <div style={{ fontSize: 12, color: 'rgba(255,255,255,0.6)' }}>Please wait while we capture the dashboard.</div>
          </div>
        </div>
      )}
    </div>
  );
}
