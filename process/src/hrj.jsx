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
  green: '#2e0a4a', greenLight: '#f5edf7', greenDark: '#1f0632',
  slate: '#605E5C', bg: '#F0F2F5', card: '#FFFFFF', border: '#E1DFDD',
  orange: '#581c87', purple: '#7c3aed', red: '#a855f7',
  headerBg: '#1f0632', jkBlue: '#2e0a4a'
};

const ACCENT = ['#2e0a4a', '#4a157a', '#6b21a8', '#8b5cf6', '#a855f7', '#c084fc', '#d8b4fe'];

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
          const color = isActive || isDone ? '#2e0a4a' : 'rgba(255,255,255,0.25)';
          return (
            <div key={phase.num} style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 10, position: 'relative' }}>
              {index > 0 && (<div style={{ position: 'absolute', right: '100%', top: 16, width: 40, height: 2, background: isDone || isActive ? '#2e0a4a' : 'rgba(255,255,255,0.15)', marginRight: 10, transition: 'all 0.4s ease' }} />)}
              <div style={{ width: 32, height: 32, borderRadius: '50%', background: isDone ? '#2e0a4a' : (isActive ? 'rgba(46,10,74,0.1)' : 'transparent'), border: `2px solid ${color}`, display: 'flex', alignItems: 'center', justifyContent: 'center', color: isDone ? '#fff' : color, fontWeight: 'bold', fontSize: 14, transition: 'all 0.3s ease', boxShadow: isActive ? '0 0 12px rgba(46,10,74,0.4)' : 'none' }}>
                {isDone ? '✓' : phase.num}
              </div>
              <div style={{ color: isActive || isDone ? '#fff' : 'rgba(255,255,255,0.4)', fontSize: 13, fontWeight: isActive ? 700 : 500, transition: 'all 0.3s ease', letterSpacing: 0.5 }}>{phase.name}</div>
            </div>
          );
        })}
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 8, width: 400 }}>
        <div style={{ width: '100%', background: 'rgba(255,255,255,.15)', borderRadius: 8, height: 6, overflow: 'hidden' }}>
          <div style={{ height: '100%', borderRadius: 8, transition: 'width .4s ease', background: 'linear-gradient(90deg,#2e0a4a,#c084fc)', width: `${progress}%`, boxShadow: '0 0 12px rgba(46,10,74,.6)' }} />
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
  const accentColor = isHappy ? '#2e0a4a' : '#5c168c';
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
    <select value={value} onChange={e => onChange(e.target.value)} style={{ fontSize: 12, padding: '5px 8px', borderRadius: 4, width: '100%', border: value && value !== 'ALL' ? `1.5px solid ${C.green}` : `1px solid ${C.border}`, background: value && value !== 'ALL' ? C.greenLight : C.card, color: '#323130', outline: 'none', cursor: 'pointer', fontWeight: value && value !== 'ALL' ? 700 : 'normal' }}>
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
  <div style={{ background: C.card, borderRadius: 8, padding: '12px 14px', border: highlighted ? `1.5px solid ${C.green}` : `1px solid ${C.border}`, boxShadow: '0 2px 8px rgba(0,0,0,.05)', transition: 'all .2s', display: 'flex', flexDirection: 'column', ...style }}>
    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 8 }}>
      <div>
        <div style={{ fontSize: 13, fontWeight: 700, color: '#323130' }}>{title}</div>
        {subtitle && <div style={{ fontSize: 10, color: '#8A8886', marginTop: 2 }}>{subtitle}</div>}
      </div>
      {highlighted && onClear && (<button onClick={onClear} style={{ fontSize: 11, color: '#fff', background: C.green, border: 'none', borderRadius: 4, padding: '3px 9px', cursor: 'pointer', fontWeight: 600, flexShrink: 0 }}>Clear</button>)}
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
  { case_id: 'HRJ-01', activity: 'Offer Letter Accepted', timestamp: '2026-04-01 09:00', user: 'Sarah', status: 'Happy Path', month: 'April' },
  { case_id: 'HRJ-01', activity: 'Background Check Completed', timestamp: '2026-04-03 10:00', user: 'HR Team', status: 'Happy Path', month: 'April' },
  { case_id: 'HRJ-01', activity: 'IT Provisioning Scheduled', timestamp: '2026-04-04 14:00', user: 'IT Admin', status: 'Happy Path', month: 'April' },
  { case_id: 'HRJ-01', activity: 'Orientation Completed', timestamp: '2026-04-10 09:00', user: 'Trainer', status: 'Happy Path', month: 'April' },
  { case_id: 'HRJ-01', activity: 'Payroll Setup', timestamp: '2026-04-10 16:00', user: 'Finance', status: 'Happy Path', month: 'April' },
  { case_id: 'HRJ-01', activity: 'Active Employee Enrolled', timestamp: '2026-04-11 09:00', user: 'HR Team', status: 'Happy Path', month: 'April' },

  // Case 2: Happy Path
  { case_id: 'HRJ-02', activity: 'Offer Letter Accepted', timestamp: '2026-04-02 10:00', user: 'John', status: 'Happy Path', month: 'April' },
  { case_id: 'HRJ-02', activity: 'Background Check Completed', timestamp: '2026-04-04 11:00', user: 'HR Team', status: 'Happy Path', month: 'April' },
  { case_id: 'HRJ-02', activity: 'IT Provisioning Scheduled', timestamp: '2026-04-05 13:00', user: 'IT Admin', status: 'Happy Path', month: 'April' },
  { case_id: 'HRJ-02', activity: 'Orientation Completed', timestamp: '2026-04-12 09:00', user: 'Trainer', status: 'Happy Path', month: 'April' },
  { case_id: 'HRJ-02', activity: 'Payroll Setup', timestamp: '2026-04-12 15:00', user: 'Finance', status: 'Happy Path', month: 'April' },
  { case_id: 'HRJ-02', activity: 'Active Employee Enrolled', timestamp: '2026-04-13 09:00', user: 'HR Team', status: 'Happy Path', month: 'April' },

  // Case 3: Background Check Issue Deviation
  { case_id: 'HRJ-03', activity: 'Offer Letter Accepted', timestamp: '2026-04-03 09:00', user: 'Robert', status: 'Deviation', month: 'April' },
  { case_id: 'HRJ-03', activity: 'Background Check Issue', timestamp: '2026-04-05 10:00', user: 'HR Team', status: 'Deviation', month: 'April' },
  { case_id: 'HRJ-03', activity: 'Background Check Completed', timestamp: '2026-04-08 14:00', user: 'HR Team', status: 'Deviation', month: 'April' },
  { case_id: 'HRJ-03', activity: 'IT Provisioning Scheduled', timestamp: '2026-04-09 11:00', user: 'IT Admin', status: 'Deviation', month: 'April' },
  { case_id: 'HRJ-03', activity: 'Orientation Completed', timestamp: '2026-04-15 09:00', user: 'Trainer', status: 'Deviation', month: 'April' },
  { case_id: 'HRJ-03', activity: 'Payroll Setup', timestamp: '2026-04-15 17:00', user: 'Finance', status: 'Deviation', month: 'April' },
  { case_id: 'HRJ-03', activity: 'Active Employee Enrolled', timestamp: '2026-04-16 09:00', user: 'HR Team', status: 'Deviation', month: 'April' },

  // Case 4: IT delay Deviation
  { case_id: 'HRJ-04', activity: 'Offer Letter Accepted', timestamp: '2026-04-04 11:00', user: 'Emma', status: 'Deviation', month: 'April' },
  { case_id: 'HRJ-04', activity: 'Background Check Completed', timestamp: '2026-04-06 12:00', user: 'HR Team', status: 'Deviation', month: 'April' },
  { case_id: 'HRJ-04', activity: 'IT Provisioning Scheduled', timestamp: '2026-04-07 14:00', user: 'IT Admin', status: 'Deviation', month: 'April' },
  { case_id: 'HRJ-04', activity: 'Provisioning Delay', timestamp: '2026-04-08 09:00', user: 'IT Admin', status: 'Deviation', month: 'April' },
  { case_id: 'HRJ-04', activity: 'Orientation Completed', timestamp: '2026-04-14 09:00', user: 'Trainer', status: 'Deviation', month: 'April' },
  { case_id: 'HRJ-04', activity: 'Payroll Setup', timestamp: '2026-04-14 16:00', user: 'Finance', status: 'Deviation', month: 'April' },
  { case_id: 'HRJ-04', activity: 'Active Employee Enrolled', timestamp: '2026-04-15 09:00', user: 'HR Team', status: 'Deviation', month: 'April' },

  // Case 5: Happy Path
  { case_id: 'HRJ-05', activity: 'Offer Letter Accepted', timestamp: '2026-04-05 09:30', user: 'Michael', status: 'Happy Path', month: 'April' },
  { case_id: 'HRJ-05', activity: 'Background Check Completed', timestamp: '2026-04-07 10:30', user: 'HR Team', status: 'Happy Path', month: 'April' },
  { case_id: 'HRJ-05', activity: 'IT Provisioning Scheduled', timestamp: '2026-04-08 11:00', user: 'IT Admin', status: 'Happy Path', month: 'April' },
  { case_id: 'HRJ-05', activity: 'Orientation Completed', timestamp: '2026-04-15 09:00', user: 'Trainer', status: 'Happy Path', month: 'April' },
  { case_id: 'HRJ-05', activity: 'Payroll Setup', timestamp: '2026-04-15 15:30', user: 'Finance', status: 'Happy Path', month: 'April' },
  { case_id: 'HRJ-05', activity: 'Active Employee Enrolled', timestamp: '2026-04-16 09:00', user: 'HR Team', status: 'Happy Path', month: 'April' },

  // Case 6: Happy Path (May)
  { case_id: 'HRJ-06', activity: 'Offer Letter Accepted', timestamp: '2026-05-01 10:00', user: 'David', status: 'Happy Path', month: 'May' },
  { case_id: 'HRJ-06', activity: 'Background Check Completed', timestamp: '2026-05-03 11:00', user: 'HR Team', status: 'Happy Path', month: 'May' },
  { case_id: 'HRJ-06', activity: 'IT Provisioning Scheduled', timestamp: '2026-05-04 14:00', user: 'IT Admin', status: 'Happy Path', month: 'May' },
  { case_id: 'HRJ-06', activity: 'Orientation Completed', timestamp: '2026-05-08 09:00', user: 'Trainer', status: 'Happy Path', month: 'May' },
  { case_id: 'HRJ-06', activity: 'Payroll Setup', timestamp: '2026-05-08 16:00', user: 'Finance', status: 'Happy Path', month: 'May' },
  { case_id: 'HRJ-06', activity: 'Active Employee Enrolled', timestamp: '2026-05-09 09:00', user: 'HR Team', status: 'Happy Path', month: 'May' }
];

const HRJ_LAYOUT = {
  'Offer Letter Accepted': { step: 0, lane: 0 },
  'Background Check Completed': { step: 1, lane: 0 },
  'IT Provisioning Scheduled': { step: 2, lane: 0 },
  'Orientation Completed': { step: 3, lane: 0 },
  'Payroll Setup': { step: 4, lane: 0 },
  'Active Employee Enrolled': { step: 5, lane: 0 },
  'Background Check Issue': { step: 1, lane: -1 },
  'Provisioning Delay': { step: 2, lane: -1 }
};

/* ── FAQ Accordion Item ── */
const FaqItem = ({ q, a, bullets, accentColor }) => {
  const [open, setOpen] = useState(false);
  const accent = accentColor || '#2e0a4a';
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

const HRJ_FAQS = [
  {
    q: 'What is the HRJ Onboarding process?',
    a: 'The Onboarding process manages the entry of a new hire into the organization, ensuring administrative, technical, and operational readiness from day one. Key activities include offer acceptance, background checks, IT provisioning, orientation training, payroll setup, and active profile activation.',
    bullets: [
      { bold: 'Offer Acceptance:', rest: ' Candidate accepts the contract and starts onboarding.' },
      { bold: 'Background Check:', rest: ' Completing credentials, security, and reference verification.' },
      { bold: 'IT Provisioning:', rest: ' Setting up corporate account, email, laptop, and permissions.' },
      { bold: 'Orientation & Training:', rest: ' Introducing company culture, guidelines, and safety policies.' },
      { bold: 'Payroll & HR System Setup:', rest: ' Verification of banking details and final system registration.' },
    ],
  },
  {
    q: 'How does Process Mining improve candidate onboarding?',
    a: 'Onboarding involves coordination across HR, IT, Facilities, and Finance. Process mining highlights:',
    bullets: [
      'Delays in IT setup or equipment delivery that leave new hires idle',
      'Long cycle times for background screenings causing start date postponements',
      'Steps performed out-of-sequence (e.g., orientation before IT provisioning)',
      'Dropout points where candidates stall or fail to complete payroll registration',
    ],
  },
  {
    q: 'What key metrics (KPIs) does Process Mining track in HRJ?',
    a: 'We monitor onboarding cycle times, compliance, and readiness rates:',
    bullets: [
      { bold: 'Onboarding Completion Rate:', rest: ' % of new hires completing all steps successfully.' },
      { bold: 'IT Provisioning Duration:', rest: ' Average hours to deploy account and credentials.' },
      { bold: 'Background Check Latency:', rest: ' Average days from notice to verification sign-off.' },
      { bold: 'Payroll Setup Latency:', rest: ' Hours taken to verify and activate banking details.' },
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

export default function HRJDashboard({ currentUser, onSignOut, onBackHome }) {
  const [step, setStep] = useState('info'); // 'info' | 'choose' | 'dashboard'
  const [loading, setLoading] = useState(false);
  const [loadProg, setLoadProg] = useState(0);
  const [loadLabel, setLoadLabel] = useState('Initializing HR Joining module...');
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
    if (current && current.activeModule === 'hrj') {
      if (current.step !== undefined) setStep(current.step);
    }

    const handlePopState = (event) => {
      if (event.state && event.state.activeModule === 'hrj') {
        if (event.state.step !== undefined) setStep(event.state.step);
      }
    };

    window.addEventListener('popstate', handlePopState);
    return () => window.removeEventListener('popstate', handlePopState);
  }, []);

  useEffect(() => {
    const current = window.history.state;
    if (!current || current.activeModule !== 'hrj' || current.step !== step) {
      window.history.pushState({ activeModule: 'hrj', step }, '');
    }
  }, [step]);

  const screenshotRef = useRef(null);
  const [screenshotting, setScreenshotting] = useState(false);

  const handleScreenshot = async () => {
    if (!screenshotRef.current || screenshotting) return;
    setScreenshotting(true);
    const el = screenshotRef.current;

    // Dynamically identify the main scroll container and calculate total height
    let captureWidth = el.clientWidth;
    let captureHeight = el.clientHeight;

    const scrollEl = Array.from(el.querySelectorAll('div')).find(node => {
      const style = window.getComputedStyle(node);
      return style.overflowY === 'auto' || style.overflowY === 'scroll';
    });

    if (scrollEl) {
      captureHeight = el.clientHeight + (scrollEl.scrollHeight - scrollEl.clientHeight);
    } else {
      captureHeight = el.scrollHeight;
    }

    // Find all React Flow containers and tag them and their SVG/path descendants
    const originalNodes = [];
    const rfContainers = el.querySelectorAll('.react-flow');
    rfContainers.forEach((rf) => {
      originalNodes.push(rf);
      rf.querySelectorAll('svg, path, g, circle, rect, text').forEach((child) => {
        originalNodes.push(child);
      });
    });

    originalNodes.forEach((node, idx) => {
      node.setAttribute('data-screenshot-id', `rf-node-${idx}`);
    });

    try {
      await new Promise((resolve) => requestAnimationFrame(resolve));

      const canvas = await html2canvas(el, {
        scale: 2,
        useCORS: true,
        allowTaint: false,
        backgroundColor: '#F0F2F5',
        logging: false,
        width: captureWidth,
        height: captureHeight,
        scrollX: 0,
        scrollY: 0,
        windowWidth: captureWidth,
        windowHeight: captureHeight,
        ignoreElements: (node) => {
          if (node.id && node.id.includes('screenshot-btn')) return true;
          if (node.id === 'screenshot-loading-overlay') return true;
          if (node.classList && (node.classList.contains('no-screenshot') || node.classList.contains('screenshot-btn'))) return true;
          if (node.style?.position === 'fixed' || window.getComputedStyle(node).position === 'fixed') return true;
          return false;
        },
        onclone: (clonedDoc, clonedEl) => {
          // Resolve specific overlays or animations
          const overlay = clonedDoc.getElementById('screenshot-loading-overlay');
          if (overlay && overlay.parentNode) {
            overlay.parentNode.removeChild(overlay);
          }
          const anims = clonedDoc.querySelectorAll('animateMotion, animate');
          for (let i = 0; i < anims.length; i++) {
            const node = anims[i];
            if (node && node.parentNode) {
              node.parentNode.removeChild(node);
            }
          }

          // Traverse up from clonedEl to clear layout boundaries in the cloned document
          let curr = clonedEl;
          while (curr && curr.style) {
            curr.style.setProperty('height', 'auto', 'important');
            curr.style.setProperty('overflow', 'visible', 'important');
            curr.style.setProperty('max-height', 'none', 'important');
            curr = curr.parentNode;
          }

          // Make clonedEl itself expand fully
          clonedEl.style.setProperty('height', 'auto', 'important');
          clonedEl.style.setProperty('max-height', 'none', 'important');
          clonedEl.style.setProperty('overflow', 'visible', 'important');

          // Find the main scroll container in the cloned DOM and expand it
          const clonedScrollEl = Array.from(clonedEl.querySelectorAll('div')).find(node => {
            const style = window.getComputedStyle(node);
            return style.overflowY === 'auto' || style.overflowY === 'scroll';
          });

          if (clonedScrollEl) {
            clonedScrollEl.style.setProperty('height', 'auto', 'important');
            clonedScrollEl.style.setProperty('max-height', 'none', 'important');
            clonedScrollEl.style.setProperty('overflow', 'visible', 'important');
            clonedScrollEl.style.setProperty('overflow-x', 'visible', 'important');
            clonedScrollEl.style.setProperty('overflow-y', 'visible', 'important');
          }

          // Copy dynamic computed styles using 1-to-1 screenshot ID lookup
          const clonedNodes = clonedEl.querySelectorAll('[data-screenshot-id]');
          clonedNodes.forEach((cEl) => {
            const screenId = cEl.getAttribute('data-screenshot-id');
            const oEl = el.querySelector(`[data-screenshot-id="${screenId}"]`);
            if (oEl) {
              const oStyle = window.getComputedStyle(oEl);
              const tagName = cEl.tagName.toLowerCase();

              // 1. Positioning and layout
              cEl.style.position = oStyle.position;
              cEl.style.display = oStyle.display;
              cEl.style.flexDirection = oStyle.flexDirection;
              cEl.style.alignItems = oStyle.alignItems;
              cEl.style.justifyContent = oStyle.justifyContent;
              cEl.style.gap = oStyle.gap;

              if (oStyle.position === 'absolute' || oStyle.position === 'fixed' || oStyle.display === 'flex' || oStyle.display === 'grid' || tagName === 'div' || tagName === 'svg' || tagName === 'canvas') {
                cEl.style.width = oStyle.width;
                cEl.style.height = oStyle.height;
                cEl.style.top = oStyle.top;
                cEl.style.left = oStyle.left;
                cEl.style.right = oStyle.right;
                cEl.style.bottom = oStyle.bottom;
              }

              // 2. Transforms & scaling
              cEl.style.transform = oStyle.transform;
              cEl.style.transformOrigin = oStyle.transformOrigin;

              // 3. Visuals
              cEl.style.opacity = oStyle.opacity;
              cEl.style.overflow = oStyle.overflow;
              cEl.style.visibility = oStyle.visibility;
              cEl.style.background = oStyle.background;
              cEl.style.backgroundColor = oStyle.backgroundColor;
              cEl.style.color = oStyle.color;
              cEl.style.border = oStyle.border;
              cEl.style.borderRadius = oStyle.borderRadius;
              cEl.style.boxShadow = oStyle.boxShadow;
              cEl.style.padding = oStyle.padding;
              cEl.style.margin = oStyle.margin;
              cEl.style.boxSizing = oStyle.boxSizing;

              // 4. Typography
              cEl.style.fontFamily = oStyle.fontFamily;
              cEl.style.fontSize = oStyle.fontSize;
              cEl.style.fontWeight = oStyle.fontWeight;
              cEl.style.lineHeight = oStyle.lineHeight;
              cEl.style.textAlign = oStyle.textAlign;

              // 5. SVG specific styling
              if (tagName === 'path' || tagName === 'svg' || tagName === 'g' || tagName === 'text' || tagName === 'circle' || tagName === 'rect') {
                if (oStyle.stroke && oStyle.stroke !== 'none') {
                  cEl.setAttribute('stroke', oStyle.stroke);
                  cEl.style.stroke = oStyle.stroke;
                }
                if (oStyle.strokeWidth) {
                  cEl.setAttribute('stroke-width', oStyle.strokeWidth);
                  cEl.style.strokeWidth = oStyle.strokeWidth;
                }
                if (oStyle.strokeDasharray) {
                  cEl.setAttribute('stroke-dasharray', oStyle.strokeDasharray);
                  cEl.style.strokeDasharray = oStyle.strokeDasharray;
                }
                if (oStyle.fill) {
                  cEl.setAttribute('fill', oStyle.fill);
                  cEl.style.fill = oStyle.fill;
                }
                if (oStyle.textAnchor) {
                  cEl.setAttribute('text-anchor', oStyle.textAnchor);
                  cEl.style.textAnchor = oStyle.textAnchor;
                }
                if (oStyle.dominantBaseline) {
                  cEl.setAttribute('dominant-baseline', oStyle.dominantBaseline);
                  cEl.style.dominantBaseline = oStyle.dominantBaseline;
                }
                
                // Clean up absolute marker URLs to prevent html2canvas / SVG-in-Image CORS sandboxing failures
                const markerEnd = oStyle.markerEnd || oEl.getAttribute('marker-end');
                if (markerEnd && markerEnd !== 'none') {
                  const match = markerEnd.match(/#([^'")\s]+)/);
                  if (match) {
                    const markerId = match[1];
                    cEl.style.setProperty('marker-end', `url(#${markerId})`, 'important');
                    cEl.setAttribute('marker-end', `url(#${markerId})`);
                  } else {
                    cEl.style.setProperty('marker-end', 'none', 'important');
                    cEl.removeAttribute('marker-end');
                  }
                } else {
                  cEl.style.setProperty('marker-end', 'none', 'important');
                  cEl.removeAttribute('marker-end');
                }
              }

              // Explicitly set SVG container attributes (width & height) and overflow to prevent html2canvas collapsing
              if (tagName === 'svg') {
                const w = parseFloat(oStyle.width);
                const h = parseFloat(oStyle.height);
                if (!isNaN(w)) cEl.setAttribute('width', w);
                if (!isNaN(h)) cEl.setAttribute('height', h);
                cEl.setAttribute('overflow', 'visible');
              }

              // Replace missing SVG grid patterns with CSS background radial gradient dots
              if (cEl.classList.contains('react-flow__background')) {
                cEl.style.setProperty('background-image', 'radial-gradient(#ccc 1.2px, transparent 1.2px)', 'important');
                cEl.style.setProperty('background-size', '20px 20px', 'important');
                cEl.style.setProperty('background-color', '#FAFAFA', 'important');
              }
            }
          });
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
          link.download = `HRJ_Dashboard_Report_${new Date().toISOString().slice(0, 10)}.png`;
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
      // Clean up original element attributes
      originalNodes.forEach((node) => {
        node.removeAttribute('data-screenshot-id');
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

  // Trigger loading phase
  const handleLoadDemo = () => {
    setLoading(true);
    setLoadProg(0);
    setLoadLabel('Initializing HR Joining module...');

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
        if (next > 40 && next < 80) setLoadLabel('Analysing background checks and IT equipment delays...');
        if (next >= 80 && next < 100) setLoadLabel('Mapping onboarding milestones...');
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
    const nodes = Object.keys(graphData.nodeFreq).map((act, index) => {
      const layout = HRJ_LAYOUT[act] || { step: 0, lane: 2 };
      const isHappy = !['Background Check Issue', 'Provisioning Delay'].includes(act);
      const x = layout.step * stepGap + 40 + (index * 0.1);
      const y = layout.lane * laneGap + 180 + (index * 0.1);
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

      const isHappyEdge = !['Background Check Issue', 'Provisioning Delay'].includes(src) &&
        !['Background Check Issue', 'Provisioning Delay'].includes(tgt);
      const color = isHappyEdge ? '#2e0a4a' : '#5c168c';

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
    let bgIssueCases = 0;
    let itDelayCases = 0;

    Object.keys(casesMap).forEach(cid => {
      const cEvents = casesMap[cid];
      const acts = cEvents.map(e => e.activity);
      if (acts.includes('Active Employee Enrolled')) completedCount++;
      if (acts.includes('Background Check Issue')) bgIssueCases++;
      if (acts.includes('Provisioning Delay')) itDelayCases++;

      if (cEvents.length > 1) {
        const start = new Date(cEvents[0].timestamp);
        const end = new Date(cEvents[cEvents.length - 1].timestamp);
        totalDays += (end - start) / (1000 * 60 * 60 * 24);
      }
    });

    const avgCycle = totalCases ? (totalDays / totalCases).toFixed(1) : '0';
    const bgSuccessRate = totalCases ? (((totalCases - bgIssueCases) / totalCases) * 100).toFixed(0) : '100';
    const itSlaRate = totalCases ? (((totalCases - itDelayCases) / totalCases) * 100).toFixed(0) : '100';

    return {
      cases: totalCases,
      cycle: `${avgCycle} Days`,
      bgSuccess: `${bgSuccessRate}%`,
      itSla: `${itSlaRate}%`
    };
  }, [casesMap]);

  // Chart: Happy path vs deviations
  const donutData = useMemo(() => {
    let happy = 0;
    let dev = 0;
    Object.keys(casesMap).forEach(cid => {
      const acts = casesMap[cid].map(e => e.activity);
      if (acts.includes('Background Check Issue') || acts.includes('Provisioning Delay')) {
        dev++;
      } else {
        happy++;
      }
    });
    return [
      { name: 'Happy Path', value: happy, color: '#2e0a4a' },
      { name: 'Deviations', value: dev, color: '#5c168c' }
    ];
  }, [casesMap]);

  // Chart: Activity Frequency
  const barChartData = useMemo(() => {
    return Object.keys(graphData.nodeFreq).map(act => ({
      name: act,
      count: graphData.nodeFreq[act]
    })).sort((a, b) => b.count - a.count);
  }, [graphData]);

  // Chart: Monthly Hires Volume
  const monthlyData = useMemo(() => {
    const months = {};
    RAW_EVENTS.forEach(e => {
      if (e.activity === 'Offer Letter Accepted') {
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

  /* ── HRJ Intro helpers ── */
  const hrjSteps = [
    'Offer Letter Accepted — Candidate accepts formal offer and contract signing verified.',
    'Background Check Completed — Credentials, records, and reference checks processed.',
    'IT Provisioning Scheduled — Accounts created and hardware/software resource scheduling initiated.',
    'Orientation Completed — Interactive welcome session and policy guidelines training finished.',
    'Payroll Setup — Bank details verified and compensation details recorded in payroll core.',
    'Active Employee Enrolled — Final HR database record created and system profile activated.',
  ];
  const hrjKpis = [
    'Onboarding completion rate',
    'IT provisioning cycle time',
    'Background check processing duration',
    'Equipment delivery lag',
    'First-day orientation rate',
    'Payroll setup latency',
  ];
  const hrjShortSteps = [
    { icon: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07A19.5 19.5 0 0 1 4.69 12a19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 3.6 1h3a2 2 0 0 1 2 1.72c.127.96.361 1.903.7 2.81a2 2 0 0 1-.45 2.11L8.09 8a16 16 0 0 0 6 6l.81-.81a2 2 0 0 1 2.11-.45c.907.339 1.85.573 2.81.7A2 2 0 0 1 22 16.92z" /></svg>, text: 'Offer' },
    { icon: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14" /><polyline points="22 4 12 14.01 9 11.01" /></svg>, text: 'BGCheck' },
    { icon: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><rect x="2" y="3" width="20" height="14" rx="2" ry="2" /><line x1="8" y1="21" x2="16" y2="21" /><line x1="12" y1="17" x2="12" y2="21" /></svg>, text: 'IT Setup' },
    { icon: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2" /><circle cx="9" cy="7" r="4" /><path d="M23 21v-2a4 4 0 0 0-3-3.87" /><path d="M16 3.13a4 4 0 0 1 0 7.75" /></svg>, text: 'Orientation' },
    { icon: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><rect x="1" y="4" width="22" height="16" rx="2" ry="2" /><line x1="1" y1="10" x2="23" y2="10" /></svg>, text: 'Payroll' },
    { icon: <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2" /><circle cx="12" cy="7" r="4" /></svg>, text: 'Active' },
  ];
  const HRJPremiumMetricAnimation = ({ num }) => (
    <div style={{ position: 'relative', width: '60px', height: '60px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
      <motion.div animate={{ scale: [1, 1.6], opacity: [0.6, 0] }} transition={{ duration: 2, repeat: Infinity, ease: 'easeOut' }} style={{ position: 'absolute', width: '40px', height: '40px', borderRadius: '50%', border: '2px solid rgba(255,255,255,0.8)' }} />
      <motion.div animate={{ scale: [1, 1.3], opacity: [0.4, 0] }} transition={{ duration: 2, repeat: Infinity, ease: 'easeOut', delay: 0.5 }} style={{ position: 'absolute', width: '40px', height: '40px', borderRadius: '50%', border: '2px solid rgba(255,255,255,0.6)' }} />
      <motion.div animate={{ rotate: 360 }} transition={{ duration: 3, repeat: Infinity, ease: 'linear' }} style={{ position: 'absolute', width: '32px', height: '32px', borderRadius: '50%', border: '2px solid transparent', borderTopColor: 'rgba(255,255,255,0.9)', borderRightColor: 'rgba(255,255,255,0.4)' }} />
      <div style={{ position: 'absolute', color: '#fff', fontWeight: 800, fontSize: '14px', textShadow: '0 0 8px rgba(255,255,255,0.8)', userSelect: 'none' }}>{num}</div>
    </div>
  );
  const HRJKeyMetricsAnimation = ({ metrics }) => {
    const colors = ['#2e0a4a', '#4a157a', '#6b21a8', '#8b5cf6', '#a855f7', '#d8b4fe'];
    const subtitles = [
      'Percent of new hires completing all tasks',
      'Hours to setup email and hardware access',
      'Days to complete security background screening',
      'Days to deliver laptops and workspace equipment',
      'Percent of hires attending welcome training',
      'Hours to verify and enroll banking details'
    ];

    const icons = [
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14" /><polyline points="22 4 12 14.01 9 11.01" /></svg>,
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><rect x="3" y="11" width="18" height="11" rx="2" ry="2" /><path d="M7 11V7a5 5 0 0 1 9.9-1" /></svg>,
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" /></svg>,
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><polyline points="21 16 12 21 3 16 12 11 21 16" /><polyline points="3 8 12 13 21 8" /><line x1="12" y1="3" x2="12" y2="13" /></svg>,
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2" /><circle cx="9" cy="7" r="4" /><path d="M23 21v-2a4 4 0 0 0-3-3.87" /><path d="M16 3.13a4 4 0 0 1 0 7.75" /></svg>,
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><rect x="1" y="4" width="22" height="16" rx="2" ry="2" /><line x1="1" y1="10" x2="23" y2="10" /></svg>
    ];

    return (
      <div style={{ display: 'flex', flexDirection: 'row', alignItems: 'center', gap: '12px', width: '100%', height: '302px' }}>
        <KpiWheel colors={colors} label="JOIN" height={302} width={140} />
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
            <p style={{ margin: '4px 0 0', fontSize: '14px', color: '#64748b' }}>Learn more about HR Onboarding (HRJ) Process Mining</p>
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
            {HRJ_FAQS.map((faq, i) => (
              <FaqItem key={i} q={faq.q} a={faq.a} bullets={faq.bullets} accentColor={C.green} />
            ))}
          </div>
        </div>
      </motion.div>
    </motion.div>
  );
  const HRJProcessTreeFlow = ({ steps }) => {
    const colors = [C.green, '#4a157a', '#6b21a8', '#8b5cf6', '#a855f7', '#d8b4fe'];
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
                background: `${C.green}22`,
                border: `1.5px solid ${C.green}`,
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
                  background: C.green,
                  color: '#fff',
                  border: 'none',
                  padding: '12px 30px',
                  borderRadius: '8px',
                  fontWeight: 700,
                  fontSize: '14px',
                  cursor: 'pointer',
                  width: '100%',
                  boxShadow: `0 4px 12px rgba(46,10,74,0.3)`,
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
            <div style={{ fontWeight: 700, fontSize: 16, color: '#fff' }}>HR Joining Process Explorer</div>
            <div style={{ fontSize: 10, color: 'rgba(255,255,255,.5)' }}>Onboarding Journey mining</div>
          </div>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <div style={{ fontSize: 12, color: 'rgba(255,255,255,0.7)' }}>
            User: <strong style={{ color: '#fff' }}>{currentUser}</strong>
          </div>
          {step === 'dashboard' && !chartsLoading && (
            <button
              id="hrj-screenshot-btn"
              onClick={handleScreenshot}
              title="Download full report screenshot"
              style={{
                width: 34, height: 34, borderRadius: '50%',
                background: screenshotting ? 'rgba(46,10,74,0.5)' : 'linear-gradient(135deg, rgba(80,30,120,0.9) 0%, rgba(46,10,74,0.95) 100%)',
                border: '1.5px solid rgba(255,255,255,0.25)',
                boxShadow: screenshotting ? '0 0 0 3px rgba(80,30,120,0.4)' : '0 2px 12px rgba(80,30,120,0.45), 0 0 0 1px rgba(255,255,255,0.08)',
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
              {[...hrjShortSteps, ...hrjShortSteps, ...hrjShortSteps, ...hrjShortSteps].map((s, i) => {
                const isFirst = i % hrjShortSteps.length === 0;
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
                    {i < hrjShortSteps.length * 4 - 1 && <div className="process-ribbon-arrow">→</div>}
                  </React.Fragment>
                );
              })}
            </div>
          </div>
          <div style={{ maxWidth: 1100, width: '100%', display: 'flex', flexDirection: 'column', gap: 16, padding: '20px 24px 30px' }}>
            <div style={{ borderBottom: '2px solid #E2E8F0', paddingBottom: 12, display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
              <div style={{ flex: 1 }}>
                <div style={{ fontSize: 11, fontWeight: 700, color: C.green, textTransform: 'uppercase', letterSpacing: 1, marginBottom: 4 }}>Process Overview</div>
                <h1 style={{ margin: '0 0 6px', fontSize: 22, fontWeight: 700, color: '#1e293b' }}>HR Joining (Onboarding)</h1>
                <p style={{ margin: 0, fontSize: 13, color: '#475569', lineHeight: 1.6, maxWidth: 900 }}>The HR Joining process monitors and optimizes candidate onboarding journeys end-to-end — from offer acceptance and background verification through IT provisioning, orientation, payroll setup, and active employee enrollment. Process mining helps reduce time-to-onboard, identify provisioning bottlenecks, and ensure compliance with onboarding SLAs.</p>
              </div>
              <button
                onClick={() => setShowFaqModal(true)}
                style={{
                  background: `linear-gradient(135deg, ${C.green} 0%, #6b21a8 100%)`,
                  color: '#fff', border: 'none', padding: '10px 20px', borderRadius: '12px',
                  fontSize: '13px', fontWeight: 700, cursor: 'pointer', display: 'flex',
                  alignItems: 'center', gap: '8px', boxShadow: `0 4px 12px rgba(46, 10, 74, 0.2)`,
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
                <HRJProcessTreeFlow steps={hrjSteps} />
              </div>

              {/* Right Column: Key Metrics */}
              <div style={{ background: '#fff', border: '1px solid #E2E8F0', borderRadius: 16, padding: '16px 20px', boxShadow: '0 4px 12px rgba(0,0,0,0.04)', display: 'flex', flexDirection: 'column' }}>
                <div style={{ fontSize: 11, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: 0.8, marginBottom: 12 }}>Key Metrics</div>
                <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                  <HRJKeyMetricsAnimation metrics={hrjKpis} />
                </div>
              </div>
            </div>
            <div style={{ display: 'flex', justifyContent: 'flex-end', paddingTop: 4 }}>
              <button onClick={() => setStep('choose')} style={{ background: C.green, color: '#fff', border: 'none', padding: '12px 32px', borderRadius: 8, fontSize: 14, fontWeight: 700, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 8, boxShadow: `0 4px 12px rgba(46, 10, 74, 0.25)`, transition: 'all 0.2s' }} onMouseOver={e => { e.currentTarget.style.filter = 'brightness(1.1)'; e.currentTarget.style.transform = 'translateY(-1px)'; }} onMouseOut={e => { e.currentTarget.style.filter = 'none'; e.currentTarget.style.transform = 'none'; }}>
                Continue <span style={{ fontSize: 16 }}>→</span>
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
          background: 'radial-gradient(circle at top right, rgba(46,10,74,0.08), transparent 60%)',
          position: 'relative'
        }}>
          {/* Snapshot Button */}
          <button
            onClick={() => window.open('/snapshot hrj.pdf', '_blank')}
            style={{
              position: 'absolute',
              top: '24px',
              right: '24px',
              background: C.green,
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
              boxShadow: '0 4px 12px rgba(46,10,74,0.3)',
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
              Select how you want to ingest event logs into the HR Joining model
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
              animate={{ borderColor: hoveredSide === 'build' ? C.green : 'rgba(255, 255, 255, 0.4)' }}
            >
              <motion.div layout style={{
                width: '56px',
                height: '56px',
                background: `${C.green}10`,
                borderRadius: '10px',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                border: `1.5px solid ${C.green}30`,
                flexShrink: 0
              }}>
                <svg width="26" height="26" viewBox="0 0 24 24" fill="none" stroke={C.green} strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round">
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
                      automatically build the process event log.                    </motion.p>
                  )}
                </AnimatePresence>
              </motion.div>
              <motion.button
                layout
                onClick={(e) => { e.stopPropagation(); setShowAvailablePopup(true); }}
                style={{
                  background: C.green,
                  color: '#fff',
                  border: 'none',
                  padding: '11px 28px',
                  borderRadius: '7px',
                  fontWeight: 700,
                  fontSize: '13px',
                  cursor: 'pointer',
                  width: '100%',
                  marginTop: 'auto',
                  transition: 'all 0.2s'
                }}
                onMouseOver={e => { e.currentTarget.style.background = C.greenDark; }}
                onMouseOut={e => { e.currentTarget.style.background = C.green; }}
              >
                {hoveredSide === 'csv' ? 'Connect' : 'Connect Database'}
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
              animate={{ borderColor: hoveredSide === 'csv' ? C.green : 'rgba(255, 255, 255, 0.4)' }}
            >
              <motion.div layout style={{
                width: '56px',
                height: '56px',
                background: `${C.green}10`,
                borderRadius: '10px',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                border: `1.5px solid ${C.green}30`,
                flexShrink: 0
              }}>
                <svg width="26" height="26" viewBox="0 0 24 24" fill="none" stroke={C.green} strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round">
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
                  background: C.green,
                  color: '#fff',
                  border: 'none',
                  padding: '11px 28px',
                  borderRadius: '7px',
                  fontWeight: 700,
                  fontSize: '13px',
                  cursor: 'pointer',
                  width: '100%',
                  marginTop: 'auto',
                  transition: 'all 0.2s'
                }}
                onMouseOver={e => { e.currentTarget.style.background = C.greenDark; }}
                onMouseOut={e => { e.currentTarget.style.background = C.green; }}
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
              <FilterSelect label="Initiator" value={selected.user} options={slicerOptions.user} onChange={val => setSelected(prev => ({ ...prev, user: val }))} />
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
            <div style={{ background: C.card, borderRadius: 6, padding: '12px', borderLeft: `4px solid ${C.green}`, boxShadow: '0 2px 6px rgba(0,0,0,.05)' }}>
              <div style={{ fontSize: 10, fontWeight: 600, color: C.green, textTransform: 'uppercase', marginBottom: 2 }}>New Hires</div>
              <div style={{ fontSize: 20, fontWeight: 700, color: '#000' }}>{kpis.cases}</div>
            </div>
            <div style={{ background: C.card, borderRadius: 6, padding: '12px', borderLeft: `4px solid ${C.orange}`, boxShadow: '0 2px 6px rgba(0,0,0,.05)' }}>
              <div style={{ fontSize: 10, fontWeight: 600, color: C.orange, textTransform: 'uppercase', marginBottom: 2 }}>Avg Time-to-Onboard</div>
              <div style={{ fontSize: 20, fontWeight: 700, color: '#000' }}>{kpis.cycle}</div>
            </div>
            <div style={{ background: C.card, borderRadius: 6, padding: '12px', borderLeft: `4px solid ${C.purple}`, boxShadow: '0 2px 6px rgba(0,0,0,.05)' }}>
              <div style={{ fontSize: 10, fontWeight: 600, color: C.purple, textTransform: 'uppercase', marginBottom: 2 }}>BG Check Pass Rate</div>
              <div style={{ fontSize: 20, fontWeight: 700, color: '#000' }}>{kpis.bgSuccess}</div>
            </div>
            <div style={{ background: C.card, borderRadius: 6, padding: '12px', borderLeft: `4px solid ${C.red}`, boxShadow: '0 2px 6px rgba(0,0,0,.05)' }}>
              <div style={{ fontSize: 10, fontWeight: 600, color: C.red, textTransform: 'uppercase', marginBottom: 2 }}>IT SLA Compliance</div>
              <div style={{ fontSize: 20, fontWeight: 700, color: '#000' }}>{kpis.itSla}</div>
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
                background: C.green, display: 'flex', justifyContent: 'space-between', alignItems: 'center'
              }}>
                <div>
                  <div style={{ fontSize: 13, fontWeight: 700, color: '#fff' }}>Onboarding Flow Map</div>
                  <div style={{ fontSize: 10, color: 'rgba(255,255,255,0.7)' }}>Employee Joining Steps</div>
                </div>
                <div style={{ display: 'flex', gap: 4, background: 'rgba(255,255,255,0.2)', padding: 2, borderRadius: 4 }}>
                  <button onClick={() => setLayoutDir('LR')} style={{
                    fontSize: 11, padding: '4px 8px', border: 'none', cursor: 'pointer', borderRadius: 3,
                    background: layoutDir === 'LR' ? '#fff' : 'transparent',
                    color: layoutDir === 'LR' ? C.green : '#fff', fontWeight: 700
                  }}>Horizontal</button>
                  <button onClick={() => setLayoutDir('TB')} style={{
                    fontSize: 11, padding: '4px 8px', border: 'none', cursor: 'pointer', borderRadius: 3,
                    background: layoutDir === 'TB' ? '#fff' : 'transparent',
                    color: layoutDir === 'TB' ? C.green : '#fff', fontWeight: 700
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
              <ChartCard title="Onboarding Funnel status" loading={chartsLoading} skeletonType="pie" style={{ flex: 1 }}>
                <ResponsiveContainer width="100%" height={180}>
                  <PieChart>
                    <Pie data={donutData} dataKey="value" cx="50%" cy="50%" innerRadius={45} outerRadius={65} paddingAngle={3}>
                      {donutData.map((entry, index) => (
                        <Cell key={index} fill={entry.color} />
                      ))}
                    </Pie>
                    <Legend verticalAlign="bottom" height={24} iconSize={10} style={{ fontSize: 11 }} />
                    <ReChartsTooltip formatter={(v) => [`${v} hires`, 'Volume']} />
                  </PieChart>
                </ResponsiveContainer>
              </ChartCard>

              <ChartCard title="Activity Occurrence" loading={chartsLoading} skeletonType="bar-horizontal" style={{ flex: 1.2 }}>
                <ResponsiveContainer width="100%" height={200}>
                  <BarChart data={barChartData} layout="vertical" margin={{ left: 10, right: 10, top: 5, bottom: 5 }}>
                    <XAxis type="number" stroke="#888" fontSize={9} />
                    <YAxis dataKey="name" type="category" stroke="#888" fontSize={8} width={110} tickFormatter={(tick) => tick.length > 22 ? `${tick.substring(0, 20)}...` : tick} />
                    <ReChartsTooltip formatter={(v) => [`${v} cases`, 'Occurrences']} />
                    <Bar dataKey="count" fill={C.green} radius={[0, 4, 4, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </ChartCard>
            </div>
          </div>

          {/* Hiring Volume + Case Table */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
            <ChartCard title="Monthly Hiring Trend" loading={chartsLoading} skeletonType="line">
              <ResponsiveContainer width="100%" height={260}>
                <LineChart data={monthlyData} margin={{ left: 5, right: 20, top: 10, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" opacity={0.3} />
                  <XAxis dataKey="month" stroke="#888" fontSize={10} />
                  <YAxis stroke="#888" fontSize={10} />
                  <ReChartsTooltip />
                  <Line type="monotone" dataKey="volume" stroke={C.green} strokeWidth={3} activeDot={{ r: 8 }} />
                </LineChart>
              </ResponsiveContainer>
            </ChartCard>

            <ChartCard title="Hiring Cases List & Checklist" loading={chartsLoading} skeletonType="timeline">
              {selectedCaseId === 'ALL' ? (
                <div style={{ overflowY: 'auto', height: 260 }}>
                  <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px' }}>
                    <thead style={{ position: 'sticky', top: 0, background: '#F0F2F5', zIndex: 1 }}>
                      <tr style={{ borderBottom: `1px solid ${C.border}` }}>
                        <th style={{ padding: '8px', textAlign: 'left', color: C.slate }}>Employee ID</th>
                        <th style={{ padding: '8px', textAlign: 'left', color: C.slate }}>Type</th>
                        <th style={{ padding: '8px', textAlign: 'left', color: C.slate }}>Initiator</th>
                      </tr>
                    </thead>
                    <tbody>
                      {Object.keys(casesMap).map((cid) => {
                        const firstEv = casesMap[cid][0];
                        const isHappy = !casesMap[cid].map(e => e.activity).includes('Background Check Issue') && !casesMap[cid].map(e => e.activity).includes('Provisioning Delay');
                        return (
                          <tr key={cid} onClick={() => setSelectedCaseId(cid)} style={{
                            borderBottom: `1px solid ${C.border}`, cursor: 'pointer',
                            background: '#fff'
                          }}
                            onMouseEnter={e => e.currentTarget.style.background = '#F3F2F1'}
                            onMouseLeave={e => e.currentTarget.style.background = '#fff'}
                          >
                            <td style={{ padding: '8px', fontWeight: 600, color: C.green }}>{cid}</td>
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
                    <div><span style={{ fontSize: 11, color: C.slate }}>Log for Candidate: </span><strong style={{ fontSize: 13, color: '#323130' }}>{selectedCaseId}</strong></div>
                    <button onClick={() => setSelectedCaseId('ALL')} style={{ fontSize: 10, background: C.green, color: '#fff', border: 'none', padding: '4px 8px', borderRadius: 4, cursor: 'pointer', fontWeight: 600 }}>Back to List</button>
                  </div>
                  <div style={{ overflowY: 'auto', flex: 1 }}>
                    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '11px' }}>
                      <thead style={{ position: 'sticky', top: 0, background: '#F0F2F5', zIndex: 1 }}>
                        <tr>
                          <th style={{ padding: '6px', textAlign: 'left', color: C.slate }}>Activity</th>
                          <th style={{ padding: '6px', textAlign: 'left', color: C.slate }}>Timestamp</th>
                          <th style={{ padding: '6px', textAlign: 'left', color: C.slate }}>Operator</th>
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
              borderTop: `3px solid ${C.green}`,
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

