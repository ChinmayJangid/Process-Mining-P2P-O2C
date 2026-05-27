import React, { useState, useEffect, useCallback, useRef } from 'react';
import ReactFlow, {
  Background, Controls, MiniMap,
  useNodesState, useEdgesState,
  MarkerType, Position,
  getBezierPath,
  EdgeLabelRenderer, BaseEdge, Handle,
} from 'reactflow';
import 'reactflow/dist/style.css';
import {
  BarChart, Bar, ComposedChart, Line, PieChart, Pie,
  XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Cell, Legend
} from 'recharts';
import { motion, AnimatePresence, useScroll, useSpring } from 'framer-motion';
import './App.css';

const API = 'http://localhost:8000';

const C = {
  amber: '#d97706', amberDark: '#b45309', amberLight: '#fffbeb',
  teal: '#ca8a04', red: '#A80000', purple: '#a16207',
  slate: '#605E5C', bg: '#F4F6F9', card: '#FFFFFF', border: '#E1DFDD',
  orange: '#fbbf24', green: '#f59e0b', selected: '#fffbeb', selectedBorder: '#d97706',
  headerBg: '#d97706', blue700: '#d97706',
};
const ACCENT = [
  '#d97706', '#b45309', '#ca8a04', '#92400e', '#78350f',
  '#fbbf24', '#f59e0b', '#fde68a', '#fef3c7', '#fffbeb'
];

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
    <div style={{ position: 'fixed', inset: 0, zIndex: 99999, background: 'rgba(30,27,22,0.95)', backdropFilter: 'blur(1px)', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: 36 }}>
      <style>{`@keyframes lo-pulse{0%,100%{opacity:1}50%{opacity:.45}}`}</style>
      <img src="/logo.png" alt="AJALabs Logo" style={{ height: 80, objectFit: 'contain', animation: 'lo-pulse 1.5s ease-in-out infinite' }} />
      <div style={{ display: 'flex', gap: 40, alignItems: 'center' }}>
        {phases.map((phase, index) => {
          const isActive = activeStep === phase.num;
          const isDone = activeStep > phase.num;
          const color = isActive || isDone ? '#d97706' : 'rgba(255,255,255,0.25)';
          return (
            <div key={phase.num} style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 10, position: 'relative' }}>
              {index > 0 && (<div style={{ position: 'absolute', right: '100%', top: 16, width: 40, height: 2, background: isDone || isActive ? '#d97706' : 'rgba(255,255,255,0.15)', marginRight: 10, transition: 'all 0.4s ease' }} />)}
              <div style={{ width: 32, height: 32, borderRadius: '50%', background: isDone ? '#d97706' : (isActive ? 'rgba(217,119,6,0.1)' : 'transparent'), border: `2px solid ${color}`, display: 'flex', alignItems: 'center', justifyContent: 'center', color: isDone ? '#1B2A4A' : color, fontWeight: 'bold', fontSize: 14, transition: 'all 0.3s ease', boxShadow: isActive ? '0 0 12px rgba(217,119,6,0.4)' : 'none' }}>
                {isDone ? '✓' : phase.num}
              </div>
              <div style={{ color: isActive || isDone ? '#fff' : 'rgba(255,255,255,0.4)', fontSize: 13, fontWeight: isActive ? 700 : 500, transition: 'all 0.3s ease', letterSpacing: 0.5 }}>{phase.name}</div>
            </div>
          );
        })}
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 8, width: 400 }}>
        <div style={{ width: '100%', background: 'rgba(255,255,255,.15)', borderRadius: 8, height: 6, overflow: 'hidden' }}>
          <div style={{ height: '100%', borderRadius: 8, transition: 'width .4s ease', background: 'linear-gradient(90deg,#d97706,#f59e0b)', width: `${progress}%`, boxShadow: '0 0 12px rgba(217,119,6,.6)' }} />
        </div>
        <div style={{ fontSize: 12, color: 'rgba(255,255,255,.6)' }}>{label}</div>
      </div>
    </div>
  );
};

/* ─── TOOLTIP ────────────────────────────────────────────────────────────────── */
const CustomTooltip = ({ active, payload, nameKey, labelOverride }) => {
  if (!active || !payload?.length || !payload[0]) return null;
  const entry = payload[0].payload || {};
  const name = nameKey ? (entry[nameKey] ?? '') : '';
  const val = payload[0].value;
  const uc = entry?.unique_cases;
  return (
    <div style={{ background: 'rgba(255,255,255,.98)', border: `1px solid ${C.border}`, borderRadius: 6, padding: '8px 14px', boxShadow: '0 4px 12px rgba(0,0,0,.15)', fontSize: 12, color: '#323130', maxWidth: 260, zIndex: 9999 }}>
      {name && <div style={{ fontWeight: 600, marginBottom: 4, color: C.amber, wordBreak: 'break-word' }}>{name}</div>}
      <div style={{ color: '#605E5C' }}>{labelOverride === 'cases' ? 'Cases:' : 'Events:'}&nbsp;<strong style={{ color: '#323130' }}>{val != null ? Number(val).toLocaleString() : 0}</strong></div>
      {uc != null && labelOverride !== 'cases' && (<div style={{ color: '#605E5C', marginTop: 2 }}>Unique Cases:&nbsp;<strong style={{ color: C.teal }}>{Number(uc).toLocaleString()}</strong></div>)}
    </div>
  );
};

/* ─── SWIM-LANE HELPERS ──────────────────────────────────────────────────────── */
const REVERSAL_NODES = new Set([
  // Production deviations
  'Operation Reversed', 'Goods Issued Reversed',
  'Goods transferred to subcontractor', 'Finished Goods Reversed',
  'Quality Inspection to Finished Goods',
  // Procurement reversals
  'PR Reversal Date', 'PO Reversal Date', 'GR Reversal Date', 'Invoice Reversal Date',
]);
// P2I happy path: 8 core steps (P2P shown as-is via gateway)
const HAPPY_PATH_NODES = new Set([
  'Production Order Creation', 'Order Confirmation', 'Basic Start',
  'Scheduled Start', 'Actual Start', 'Goods Issued to Order',
  'Finished Goods Receipt', 'Finished Goods to Quality Inspection',
]);
const getLaneStyle = (label, isMain) => {
  if (REVERSAL_NODES.has(label)) return { bg: '#FEF2F2', border: '#F87171', text: '#991B1B', badge: '#DC2626' };
  if (isMain || HAPPY_PATH_NODES.has(label)) return { bg: '#FFFBEB', border: '#d97706', text: '#78350F', badge: '#d97706' };
  return { bg: '#F8FAFC', border: '#94A3B8', text: '#334155', badge: '#64748B' };
};

/* ─── PROCESS NODE (p2p-style large node) ─────────────────────────────────────── */
const ProcessNode = React.memo(({ data }) => {
  const freq = data?.frequency || 0;
  const isHappy = HAPPY_PATH_NODES.has(data?.label);
  const isDeviation = REVERSAL_NODES.has(data?.label);

  /* colour scheme mirrors p2p exactly for happy-path; P2I adds red for deviations */
  const bgColor = isHappy ? '#d97706' : (isDeviation ? '#e74c3c' : '#999999');

  return (
    <div style={{
      background: bgColor,
      border: '2px solid #78350F',
      borderRadius: 8,
      minWidth: 600,
      minHeight: 220,
      padding: '16px',
      textAlign: 'center',
      boxShadow: '0 4px 8px rgba(0,0,0,.1)',
      fontFamily: "'Segoe UI', sans-serif",
      display: 'flex',
      flexDirection: 'column',
      justifyContent: 'space-between',
      alignItems: 'center',
      position: 'relative',
      zIndex: 10,
    }}>
      {/* Halo Pulse Effect (matches p2p) */}
      <motion.div
        style={{
          position: 'absolute', inset: -4, borderRadius: 12,
          border: '4px solid #f59e0b', zIndex: -1, pointerEvents: 'none'
        }}
        initial={{ opacity: 0, scale: 0.95 }}
        whileHover={{
          opacity: [0, 0.6, 0], scale: [1, 1.15, 1.35],
          transition: { duration: 1.5, repeat: Infinity, ease: 'easeOut' }
        }}
      />
      <div style={{
        fontSize: 52, fontWeight: 700, color: '#102A43',
        lineHeight: 1.3, marginBottom: 8, wordBreak: 'break-word', width: '100%',
      }}>
        {data?.label || ''}
      </div>
      <div style={{ alignSelf: 'center' }}>
        <span style={{
          fontSize: 38, fontWeight: 600, color: '#92400e',
          backgroundColor: 'rgba(255,255,255,0.7)', borderRadius: 12,
          padding: '3px 12px', border: '1px solid rgba(0,0,0,0.08)', whiteSpace: 'nowrap'
        }}>
          {freq > 0 ? Number(freq).toLocaleString() : '0'} cases
        </span>
      </div>
      <Handle type="target" id="top-t" position={Position.Top} style={{ opacity: 0 }} />
      <Handle type="source" id="top-s" position={Position.Top} style={{ opacity: 0 }} />
      <Handle type="target" id="bottom-t" position={Position.Bottom} style={{ opacity: 0 }} />
      <Handle type="source" id="bottom-s" position={Position.Bottom} style={{ opacity: 0 }} />
      <Handle type="target" id="left-t" position={Position.Left} style={{ opacity: 0 }} />
      <Handle type="source" id="left-s" position={Position.Left} style={{ opacity: 0 }} />
      <Handle type="target" id="right-t" position={Position.Right} style={{ opacity: 0 }} />
      <Handle type="source" id="right-s" position={Position.Right} style={{ opacity: 0 }} />
    </div>
  );
});

/* ─── P2P GATEWAY NODE ───────────────────────────────────────────────────────── */
const P2pGatewayNode = React.memo(() => (
  <div style={{
    background: 'linear-gradient(135deg,#78350F,#92400E)', border: '2.5px solid #FBBF24',
    borderRadius: 10, minWidth: 600, minHeight: 220, padding: '20px 24px',
    boxShadow: '0 4px 16px rgba(245,158,11,.4)',
    fontFamily: "'Segoe UI',sans-serif", cursor: 'pointer', textAlign: 'center',
    display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: 12,
  }}
    title="Double-click to open P2P dashboard">
    <div style={{ fontSize: 48, marginBottom: 4 }}>🔗</div>
    <div style={{ fontSize: 40, fontWeight: 800, color: '#fff', letterSpacing: .3 }}>P2P Process</div>
    <div style={{ fontSize: 24, color: 'rgba(255,255,255,.6)', marginTop: 4 }}>Double-click to explore</div>
    <Handle type="target" id="left-t" position={Position.Left} style={{ opacity: 0 }} />
    <Handle type="source" id="right-s" position={Position.Right} style={{ opacity: 0 }} />
    <Handle type="target" id="top-t" position={Position.Top} style={{ opacity: 0 }} />
    <Handle type="source" id="bottom-s" position={Position.Bottom} style={{ opacity: 0 }} />
  </div>
));

/* ─── FREQ EDGE ──────────────────────────────────────────────────────────────── */
const cubicBezierPoint = (p0, p1, p2, p3, t) => {
  const mt = 1 - t;
  return mt * mt * mt * p0 + 3 * mt * mt * t * p1 + 3 * mt * t * t * p2 + t * t * t * p3;
};

const FreqEdge = React.memo(({ id, sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, data, markerEnd, style }) => {
  const curvature = data?.curvature ?? 0.5;
  const sweepSide = data?.sweepSide;
  const sweepDist = data?.sweepDist ?? 120;
  const freq = data?.frequency || 0;
  const max = data?.maxFreq || 1;
  const width = 1 + (freq / max) * 4;
  const arcColor = '#605E5C';
  let edgePath, labelX, labelY;
  if (sweepSide) {
    let cx1, cy1, cx2, cy2;
    if (sweepSide === 'right') { cx1 = sourceX + sweepDist; cy1 = sourceY; cx2 = targetX + sweepDist; cy2 = targetY; }
    else if (sweepSide === 'left') { cx1 = sourceX - sweepDist; cy1 = sourceY; cx2 = targetX - sweepDist; cy2 = targetY; }
    else if (sweepSide === 'top') { cx1 = sourceX; cy1 = sourceY - sweepDist; cx2 = targetX; cy2 = targetY - sweepDist; }
    else { cx1 = sourceX; cy1 = sourceY + sweepDist; cx2 = targetX; cy2 = targetY + sweepDist; }
    edgePath = `M ${sourceX} ${sourceY} C ${cx1} ${cy1}, ${cx2} ${cy2}, ${targetX} ${targetY}`;
    const t = 0.65;
    labelX = cubicBezierPoint(sourceX, cx1, cx2, targetX, t);
    labelY = cubicBezierPoint(sourceY, cy1, cy2, targetY, t);
    const tx = cubicBezierPoint(sourceX, cx1, cx2, targetX, t + 0.01) - labelX;
    const ty = cubicBezierPoint(sourceY, cy1, cy2, targetY, t + 0.01) - labelY;
    const len = Math.sqrt(tx * tx + ty * ty) || 1;
    labelX += (-ty / len) * 14; labelY += (tx / len) * 14;
  } else {
    [edgePath, labelX, labelY] = getBezierPath({ sourceX, sourceY, sourcePosition, targetX, targetY, targetPosition, curvature });
    labelX = sourceX + (labelX - sourceX) * 1.3;
    labelY = sourceY + (labelY - sourceY) * 1.3;
    const clamp = (v, a, b) => Math.min(Math.max(v, Math.min(a, b)), Math.max(a, b));
    labelX = clamp(labelX, sourceX, targetX);
    labelY = clamp(labelY, sourceY, targetY);
    const dx = targetX - sourceX; const dy = targetY - sourceY;
    const len = Math.sqrt(dx * dx + dy * dy) || 1;
    labelX += (-dy / len) * 14; labelY += (dx / len) * 14;
  }
  const duration = Math.max(4, 10 - (freq / max) * 6);
  return (
    <>
      <BaseEdge id={id} path={edgePath} markerEnd={markerEnd} style={{ ...style, stroke: arcColor, strokeWidth: width, opacity: .85 }} />
      <path d={edgePath} fill="none" stroke="#d97706" strokeWidth={Math.max(4, width / 1.5)} strokeDasharray="1 20" strokeLinecap="round" style={{ opacity: 0.6, animation: `cometFlow ${duration}s linear infinite` }} />
      {[0, 1, 2].map((i) => (
        <path key={i} d="M -8,-6 L 8,0 L -8,6 Z" fill="#d97706" style={{ opacity: 0 }}>
          <animateMotion dur={`${duration}s`} repeatCount="indefinite" path={edgePath} rotate="auto" begin={`${i * (duration / 3)}s`} />
          <animate attributeName="opacity" values="0;1;1;0" keyTimes="0;0.1;0.9;1" dur={`${duration}s`} repeatCount="indefinite" begin={`${i * (duration / 3)}s`} />
        </path>
      ))}
      {freq > 0 && (
        <EdgeLabelRenderer>
          <div style={{ position: 'absolute', transform: `translate(-50%,-50%) translate(${labelX}px,${labelY}px)`, pointerEvents: 'all', zIndex: 100, display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 2 }}>
            <div style={{ fontSize: 25, fontWeight: 700, color: '#323130', background: 'rgba(255,255,255,0.95)', border: '1px solid #E1DFDD', padding: '1px 6px', borderRadius: 4, boxShadow: '0 2px 4px rgba(0,0,0,0.12)' }}>{Number(freq).toLocaleString()}</div>
            {data?.avg_days != null && (<div style={{ fontSize: 25, color: '#605E5C', background: 'rgba(255,255,255,.9)', padding: '0 4px', borderRadius: 2 }}>{data.avg_days}d</div>)}
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  );
});

const P2P_GATEWAY_ID = '__p2p_gateway__';
const nodeTypes = { processNode: ProcessNode, p2pGateway: P2pGatewayNode };
const edgeTypes = { freqEdge: FreqEdge };

/* ─── HAPPY PATH for P2I ─────────────────────────────────────────────────────── */
const HAPPY_PATH = [
  'Production Order Creation', 'Order Confirmation', 'Basic Start',
  'Scheduled Start', 'Actual Start', 'Goods Issued to Order',
  'Finished Goods Receipt', 'Finished Goods to Quality Inspection',
];
const HAPPY_IDX = Object.fromEntries(HAPPY_PATH.map((n, i) => [n, i]));

/* ─── FIXED SWIM-LANE POSITIONS ──────────────────────────────────────────────
   Large-node layout matching p2p visual style exactly.
   Nodes are 600px wide × 220px tall minimum.
   LR : Happy path y=340  | ABOVE (production deviations) y=80 / y=-180 stacked
   TB : Happy path x=650  | LEFT  (production deviations) x=60 / x=-580 stacked
        RIGHT (P2P branch) x=1300 / x=1950 for reversals
─────────────────────────────────────────────────────────────────────────────── */
const NODE_W = 750;  // 600px node + 150px gap (LR step between happy-path nodes)
const NODE_H = 310;  // 220px node + 90px gap  (TB step between nodes)

const FIXED_POS_LR = {
  /* ── Happy Path (middle lane y = 340) — 8 P2I steps ─────────────────── */
  'Production Order Creation': { x: 50, y: 340 },
  'Order Confirmation': { x: 50 + NODE_W, y: 340 },
  'Basic Start': { x: 50 + NODE_W * 2, y: 340 },
  'Scheduled Start': { x: 50 + NODE_W * 3, y: 340 },
  'Actual Start': { x: 50 + NODE_W * 4, y: 340 },
  'Goods Issued to Order': { x: 50 + NODE_W * 5, y: 340 },
  'Finished Goods Receipt': { x: 50 + NODE_W * 6, y: 340 },
  'Finished Goods to Quality Inspection': { x: 50 + NODE_W * 7, y: 340 },
  /* ── ABOVE lane — 5 deviations (y=60 / y=-220 for stacked pair) ─────── */
  'Operation Reversed': { x: 50 + NODE_W, y: 60 },  // above Order Confirmation
  'Goods Issued Reversed': { x: 50 + NODE_W * 5, y: 60 },  // above Goods Issued
  'Goods transferred to subcontractor': { x: 50 + NODE_W * 5, y: -220 },  // stacked above GI Reversed
  'Finished Goods Reversed': { x: 50 + NODE_W * 6, y: 60 },  // above FG Receipt
  'Quality Inspection to Finished Goods': { x: 50 + NODE_W * 7, y: 60 },  // above FG→QI
  /* ── P2P Branch (y = 340, right of FG→QI — shown as-is) ─────────────── */
  'PR Creation': { x: 50 + NODE_W * 8, y: 340 },
  'PR Release Date': { x: 50 + NODE_W * 9, y: 340 },
  'PO Creation': { x: 50 + NODE_W * 10, y: 340 },
  'PO Date': { x: 50 + NODE_W * 11, y: 340 },
  'GR Posting': { x: 50 + NODE_W * 12, y: 340 },
  'Invoice Posting': { x: 50 + NODE_W * 13, y: 340 },
  'PR Reversal Date': { x: 50 + NODE_W * 9, y: 60 },
  'PO Reversal Date': { x: 50 + NODE_W * 11, y: 60 },
  'GR Reversal Date': { x: 50 + NODE_W * 12, y: 60 },
  'Invoice Reversal Date': { x: 50 + NODE_W * 13, y: 60 },
};

const FIXED_POS_TB = {
  /* ── Centre lane (x = 750) — 8 happy-path steps ────────────────────── */
  'Production Order Creation': { x: 750, y: 50 },
  'Order Confirmation': { x: 750, y: 50 + NODE_H },
  'Basic Start': { x: 750, y: 50 + NODE_H * 2 },
  'Scheduled Start': { x: 750, y: 50 + NODE_H * 3 },
  'Actual Start': { x: 750, y: 50 + NODE_H * 4 },
  'Goods Issued to Order': { x: 750, y: 50 + NODE_H * 5 },
  'Finished Goods Receipt': { x: 750, y: 50 + NODE_H * 6 },
  'Finished Goods to Quality Inspection': { x: 750, y: 50 + NODE_H * 7 },
  /* ── Left lane (x=0 / x=-750 for stacked pair) — 5 deviations ─────── */
  'Operation Reversed': { x: 0, y: 50 + NODE_H },      // beside Order Confirmation
  'Goods Issued Reversed': { x: 0, y: 50 + NODE_H * 5 },  // beside Goods Issued
  'Goods transferred to subcontractor': { x: -750, y: 50 + NODE_H * 5 },  // stacked left of Sub Con
  'Finished Goods Reversed': { x: 0, y: 50 + NODE_H * 6 },  // beside FG Receipt
  'Quality Inspection to Finished Goods': { x: 0, y: 50 + NODE_H * 7 },  // beside FG→QI
  /* ── Right lane (x=1500 / x=2250 for reversals) — P2P shown as-is ─── */
  'PR Creation': { x: 1500, y: 50 },
  'PR Release Date': { x: 1500, y: 50 + NODE_H },
  'PR Reversal Date': { x: 2250, y: 50 + NODE_H },
  'PO Creation': { x: 1500, y: 50 + NODE_H * 2 },
  'PO Date': { x: 1500, y: 50 + NODE_H * 3 },
  'PO Reversal Date': { x: 2250, y: 50 + NODE_H * 3 },
  'GR Posting': { x: 1500, y: 50 + NODE_H * 4 },
  'GR Reversal Date': { x: 2250, y: 50 + NODE_H * 4 },
  'Invoice Posting': { x: 1500, y: 50 + NODE_H * 5 },
  'Invoice Reversal Date': { x: 2250, y: 50 + NODE_H * 5 },
};

/* ─── SIDE-LANE SETS ── */
const SIDE_ABOVE_LR = new Set([           // production/execution deviations → ABOVE in LR mode
  'Operation Reversed', 'Goods Issued Reversed',
  'Goods transferred to subcontractor', 'Finished Goods Reversed',
  'Quality Inspection to Finished Goods',
]);
const SIDE_BELOW_LR = new Set([           // procurement reversals → BELOW in LR mode
  'PR Reversal Date', 'PO Reversal Date', 'GR Reversal Date', 'Invoice Reversal Date',
]);
const SIDE_LEFT_TB = new Set([            // production/execution deviations → LEFT in TB mode
  'Operation Reversed', 'Goods Issued Reversed',
  'Goods transferred to subcontractor', 'Finished Goods Reversed',
  'Quality Inspection to Finished Goods',
]);
const SIDE_RIGHT_TB = new Set([           // procurement reversals → RIGHT in TB mode
  'PR Reversal Date', 'PO Reversal Date', 'GR Reversal Date', 'Invoice Reversal Date',
]);

/* ─── classifyEdge — exact P2P logic adapted for P2I node names ──────────── */
const classifyEdge = (src, tgt, sPos, tPos, dir) => {
  const sIdx = HAPPY_IDX[src];
  const tIdx = HAPPY_IDX[tgt];
  const sIsHappy = sIdx !== undefined;
  const tIsHappy = tIdx !== undefined;

  /* Happy-path → Happy-path edges */
  if (sIsHappy && tIsHappy) {
    const steps = tIdx - sIdx;
    if (steps === 1) {
      return dir === 'LR'
        ? { sh: 'right-s', th: 'left-t', curvature: 0.1 }
        : { sh: 'bottom-s', th: 'top-t', curvature: 0.1 };
    }
    if (steps > 1) {
      const sweepDist = 150 + steps * 120;
      return dir === 'LR'
        ? { sh: 'top-s', th: 'top-t', sweepSide: 'top', sweepDist }
        : { sh: 'right-s', th: 'right-t', sweepSide: 'right', sweepDist };
    }
    if (steps < 0) {
      const sweepDist = 150 + Math.abs(steps) * 120;
      return dir === 'LR'
        ? { sh: 'bottom-s', th: 'bottom-t', sweepSide: 'bottom', sweepDist }
        : { sh: 'left-s', th: 'left-t', sweepSide: 'left', sweepDist };
    }
  }

  /* Deviation ↔ Happy-path edges — same routing rules as P2P */
  if (dir === 'LR') {
    const srcAbove = SIDE_ABOVE_LR.has(src), tgtAbove = SIDE_ABOVE_LR.has(tgt);
    const srcBelow = SIDE_BELOW_LR.has(src), tgtBelow = SIDE_BELOW_LR.has(tgt);
    if (sIsHappy && tgtAbove) return { sh: 'top-s', th: 'bottom-t', curvature: 0.5 };
    if (srcAbove && tIsHappy) return { sh: 'bottom-s', th: 'top-t', curvature: 0.5 };
    if (sIsHappy && tgtBelow) return { sh: 'bottom-s', th: 'top-t', curvature: 0.5 };
    if (srcBelow && tIsHappy) return { sh: 'top-s', th: 'bottom-t', curvature: 0.5 };
    if ((srcAbove || srcBelow) && (tgtAbove || tgtBelow))
      return { sh: 'right-s', th: 'left-t', curvature: 0.4 };
  } else {
    const srcLeft = SIDE_LEFT_TB.has(src), tgtLeft = SIDE_LEFT_TB.has(tgt);
    const srcRight = SIDE_RIGHT_TB.has(src), tgtRight = SIDE_RIGHT_TB.has(tgt);
    if (sIsHappy && tgtLeft) return { sh: 'left-s', th: 'right-t', curvature: 0.5 };
    if (srcLeft && tIsHappy) return { sh: 'right-s', th: 'left-t', curvature: 0.5 };
    if (sIsHappy && tgtRight) return { sh: 'right-s', th: 'left-t', curvature: 0.5 };
    if (srcRight && tIsHappy) return { sh: 'left-s', th: 'right-t', curvature: 0.5 };
    if ((srcLeft || srcRight) && (tgtLeft || tgtRight))
      return { sh: 'bottom-s', th: 'top-t', curvature: 0.4 };
  }

  /* Fallback based on relative position */
  const dx = tPos.x - sPos.x;
  const dy = tPos.y - sPos.y;
  if (dir === 'LR') {
    if (Math.abs(dy) < 80) return dx > 0
      ? { sh: 'right-s', th: 'left-t', curvature: 0.3 }
      : { sh: 'bottom-s', th: 'bottom-t', curvature: 0.5 };
    return dy > 0
      ? { sh: 'bottom-s', th: 'top-t', curvature: 0.4 }
      : { sh: 'top-s', th: 'bottom-t', curvature: 0.4 };
  } else {
    if (Math.abs(dx) < 80) return dy > 0
      ? { sh: 'bottom-s', th: 'top-t', curvature: 0.3 }
      : { sh: 'right-s', th: 'right-t', curvature: 0.5 };
    return dx > 0
      ? { sh: 'right-s', th: 'left-t', curvature: 0.4 }
      : { sh: 'left-s', th: 'right-t', curvature: 0.4 };
  }
};

const buildFlowMap = (bNodes, bEdges, setRfNodes, setRfEdges, dir) => {
  const mxF = Math.max(1, ...(bNodes || []).map(n => n.frequency || 0));
  const mxE = Math.max(1, ...(bEdges || []).map(e => e.frequency || 0));

  /* ── Track unknowns so they auto-stack in a neutral 3rd lane ── */
  const fixedMap = dir === 'LR' ? FIXED_POS_LR : FIXED_POS_TB;
  let otherX = 50, otherY = dir === 'LR' ? 700 : 50;  // 3rd lane below both deviation lanes
  const otherStep = dir === 'LR' ? NODE_W : NODE_H;

  const nodes = (bNodes || []).map(n => {
    let pos = fixedMap[n.label];
    if (!pos) {
      /* Other deviations / side activities → third lane */
      if (dir === 'LR') { pos = { x: otherX, y: 740 }; otherX += otherStep; }
      else { pos = { x: 2600, y: otherY }; otherY += otherStep; }
    }
    return {
      id: n.id, type: 'processNode', position: pos,
      data: { label: n.label, is_main: n.is_main, frequency: n.frequency || 0, maxFreq: mxF }
    };
  });

  // Inject P2P gateway node — positioned after FG→QI (the last P2I happy-path step)
  const qiNode = nodes.find(n => n.id === 'Finished Goods to Quality Inspection');
  const invNode = nodes.find(n => n.id === 'Invoice Posting');
  const anchorNode = qiNode || invNode;
  const gwPos = anchorNode
    ? (dir === 'LR'
      ? { x: anchorNode.position.x + NODE_W + 100, y: anchorNode.position.y }
      : { x: anchorNode.position.x, y: anchorNode.position.y + NODE_H + 100 })
    : { x: 9000, y: 380 };
  nodes.push({ id: P2P_GATEWAY_ID, type: 'p2pGateway', position: gwPos, data: { label: 'P2P Process' } });

  const edges = (bEdges || []).map(e => {
    const sN = nodes.find(n => n.id === e.source);
    const tN = nodes.find(n => n.id === e.target);
    if (!sN || !tN) return null;
    const { sh, th, curvature, sweepSide, sweepDist } = classifyEdge(e.source, e.target, sN.position, tN.position, dir);
    const isHappyEdge = HAPPY_PATH_NODES.has(e.source) && HAPPY_PATH_NODES.has(e.target);
    const isRevEdge = REVERSAL_NODES.has(e.source) || REVERSAL_NODES.has(e.target);
    const ec = isRevEdge ? '#e74c3c' : (isHappyEdge ? '#d97706' : '#605E5C');
    return {
      id: e.id || `${e.source}--${e.target}`, source: e.source, target: e.target,
      sourceHandle: sh, targetHandle: th, type: 'freqEdge',
      markerEnd: { type: MarkerType.ArrowClosed, color: ec, width: 16, height: 16 },
      data: { frequency: e.frequency, avg_days: e.avg_days, maxFreq: mxE, curvature, sweepSide, sweepDist, edgeColor: ec }
    };
  }).filter(Boolean);

  // Gateway edge: FG→QI → P2P gateway (dashed blue/gold)
  const gatewaySourceId = qiNode ? 'Finished Goods to Quality Inspection' : 'Invoice Posting';
  const gatewaySourceNode = qiNode || invNode;
  if (gatewaySourceNode) {
    const sh = dir === 'LR' ? 'right-s' : 'bottom-s';
    const th = dir === 'LR' ? 'left-t' : 'top-t';
    edges.push({
      id: `${gatewaySourceId}--${P2P_GATEWAY_ID}`,
      source: gatewaySourceId, target: P2P_GATEWAY_ID,
      sourceHandle: sh, targetHandle: th, type: 'freqEdge',
      markerEnd: { type: MarkerType.ArrowClosed, color: '#f59e0b', width: 16, height: 16 },
      style: { stroke: '#f59e0b', strokeWidth: 2, strokeDasharray: '6 4' },
      data: { frequency: 0, avg_days: null, maxFreq: 1, curvature: 0.05, edgeColor: '#f59e0b' }
    });
  }
  setRfNodes(nodes);
  setRfEdges(edges);
};

/* ─── HELPERS ────────────────────────────────────────────────────────────────── */
const VALID_KEYS = new Set(['company', 'plant', 'matnr', 'auart', 'kostl', 'fevor', 'ekgrp', 'lifnr', 'vendor', 'case_id', 'month', 'activity', 'year', 'quarter', 'lead_time', 'ernam', 'status']);
const qs = (params) => {
  const p = Object.entries(params).filter(([k, v]) => VALID_KEYS.has(k) && v && v !== 'ALL');
  return p.length ? '?' + p.map(([k, v]) => `${k}=${encodeURIComponent(v)}`).join('&') : '';
};
const CROSS_TO_PARAM = { company: 'company', plant: 'plant', matnr: 'matnr', auart: 'auart', kostl: 'kostl', fevor: 'fevor', ekgrp: 'ekgrp', lifnr: 'lifnr', vendor: 'vendor', case_id: 'case_id', month: 'month', activity: 'activity', year: 'year', quarter: 'quarter', lead_time: 'lead_time', ernam: 'ernam', status: 'status' };

const Skeleton = ({ width = '100%', height = '20px', borderRadius = 4, style }) => (
  <div className="skeleton-shimmer" style={{ width, height, borderRadius, ...style }} />
);
const ChartSkeleton = () => (
  <div style={{ display: 'flex', flexDirection: 'column', gap: 12, padding: 10 }}>
    <Skeleton width="60%" height="24px" />
    <Skeleton width="90%" height="150px" />
  </div>
);
const TabSkeletonCard = ({ delay = 0, tall = false }) => (
  <div className="tab-skeleton-card" style={{ animationDelay: `${delay}s` }}>
    <div className="tab-skeleton-title" style={{ animationDelay: `${delay + 0.05}s` }} />
    <div className="tab-skeleton-sub" style={{ animationDelay: `${delay + 0.1}s` }} />
    <div className="tab-skeleton-chart" style={{ height: tall ? 220 : 160, animationDelay: `${delay + 0.15}s` }} />
  </div>
);
const TabSkeletonGrid = ({ cards = 6, processTab = false }) => {
  if (processTab) return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 10 }}>
        <div className="tab-skeleton-card" style={{ height: 915 }}><div className="tab-skeleton-title" /><div className="tab-skeleton-chart" style={{ flex: 1, height: 'calc(100% - 60px)' }} /></div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>{[0, 1, 2].map(i => <TabSkeletonCard key={i} delay={i * 0.08} tall />)}</div>
      </div>
    </div>
  );
  return (<div className="tab-skeleton-grid">{Array.from({ length: cards }).map((_, i) => (<TabSkeletonCard key={i} delay={i * 0.07} tall={i >= cards - 2} />))}</div>);
};
const Empty = () => (<div style={{ height: 90, display: 'flex', alignItems: 'center', justifyContent: 'center', color: C.slate, fontSize: 12 }}>No data available</div>);
const EmptyState = ({ condition, message, children, action }) => {
  if (!condition) return children;
  return (<div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '300px', background: '#F8FAFC', borderRadius: '6px', border: '1px dashed #CBD5E1', color: '#64748b', fontSize: '13px', padding: '20px', textAlign: 'center', flexDirection: 'column', gap: '12px' }}><div style={{ fontSize: '28px', opacity: 0.8 }}>📉</div><div style={{ fontWeight: 600, maxWidth: '250px' }}>{message}</div>{action}</div>);
};

/* ─── KPI CARDS ──────────────────────────────────────────────────────────────── */
const KpiCard = React.memo(({ label, value, onClick, tooltip, highlighted }) => {
  const [hover, setHover] = useState(false);
  const bColor = hover ? 'rgba(217,119,6,0.5)' : (highlighted ? C.selectedBorder : 'transparent');
  return (
    <div onClick={onClick} onMouseEnter={() => setHover(true)} onMouseLeave={() => setHover(false)} style={{ background: highlighted ? C.selected : C.card, borderRadius: 6, padding: '10px 14px', borderTop: `1.5px solid ${bColor}`, borderRight: `1.5px solid ${bColor}`, borderBottom: `1.5px solid ${bColor}`, borderLeft: `4px solid ${C.amber}`, boxShadow: hover ? '0 6px 16px rgba(217,119,6,.15)' : '0 2px 6px rgba(0,0,0,.05)', transition: 'all .2s', cursor: onClick ? 'pointer' : 'default', minWidth: 0, position: 'relative', display: 'flex', flexDirection: 'column', alignItems: 'center', textAlign: 'center', transform: hover ? 'translateY(-3px)' : 'none', boxSizing: 'border-box', zIndex: hover ? 50 : 1 }}>
      <div style={{ fontSize: 10, fontWeight: 600, color: C.amber, textTransform: 'uppercase', letterSpacing: .5, marginBottom: 2 }}>{label}</div>
      <div style={{ fontSize: 20, fontWeight: 600, color: '#000000', lineHeight: 1 }}>{value != null ? Number(value).toLocaleString() : '—'}</div>
      {hover && tooltip && (<div style={{ position: 'absolute', top: '100%', left: 0, marginTop: 4, background: '#fff', border: `1px solid ${C.border}`, borderRadius: 4, padding: '6px 10px', boxShadow: '0 4px 12px rgba(0,0,0,.15)', fontSize: 11, color: '#323130', zIndex: 100, whiteSpace: 'nowrap', textAlign: 'left' }}>{tooltip}</div>)}
    </div>
  );
});

/* ─── SEARCHABLE SELECT ──────────────────────────────────────────────────────── */
const SearchableSelect = ({ label, value, options, onChange }) => {
  const [isOpen, setIsOpen] = useState(false);
  const [search, setSearch] = useState('');
  const dropdownRef = useRef(null);
  useEffect(() => {
    const h = (e) => { if (dropdownRef.current && !dropdownRef.current.contains(e.target)) setIsOpen(false); };
    document.addEventListener('mousedown', h);
    return () => document.removeEventListener('mousedown', h);
  }, []);
  const filtered = ['ALL', ...options].filter(o => String(o).toLowerCase().includes(search.toLowerCase()));
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 3, flex: 1, minWidth: 150, position: 'relative' }} ref={dropdownRef}>
      <label style={{ fontSize: 10, fontWeight: 700, color: '#323130', textTransform: 'uppercase', letterSpacing: .4 }}>{label}</label>
      <div onClick={() => setIsOpen(!isOpen)} style={{ fontSize: 12, padding: '5px 8px', borderRadius: 4, width: '100%', border: value && value !== 'ALL' ? `1.5px solid ${C.amber}` : `1px solid ${C.border}`, background: value && value !== 'ALL' ? C.amberLight : C.card, color: '#323130', cursor: 'pointer', fontWeight: value && value !== 'ALL' ? 700 : 'normal', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <span style={{ whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{value || 'ALL'}</span>
        <span style={{ fontSize: 10, opacity: 0.6 }}>▼</span>
      </div>
      {isOpen && (
        <div style={{ position: 'absolute', top: '100%', left: 0, right: 0, zIndex: 1000, background: '#fff', border: `1px solid ${C.border}`, borderRadius: 4, boxShadow: '0 4px 12px rgba(0,0,0,0.15)', maxHeight: 200, overflowY: 'auto', marginTop: 2 }}>
          <input type="text" placeholder="Search..." value={search} onChange={e => setSearch(e.target.value)} style={{ width: '100%', padding: '6px', border: 'none', borderBottom: `1px solid ${C.border}`, fontSize: 12, outline: 'none' }} onClick={e => e.stopPropagation()} autoFocus />
          {filtered.map((opt, i) => (<div key={i} onClick={() => { onChange(opt); setIsOpen(false); setSearch(''); }} style={{ padding: '6px 8px', fontSize: 12, cursor: 'pointer', background: value === opt ? C.amberLight : '#fff', color: value === opt ? C.amber : '#323130', borderLeft: value === opt ? `3px solid ${C.amber}` : '3px solid transparent' }} onMouseEnter={e => { if (value !== opt) e.currentTarget.style.background = '#F3F2F1'; }} onMouseLeave={e => { if (value !== opt) e.currentTarget.style.background = '#fff'; }}>{opt}</div>))}
          {filtered.length === 0 && <div style={{ padding: '8px', fontSize: 11, color: '#8A8886', textAlign: 'center' }}>No results</div>}
        </div>
      )}
    </div>
  );
};

const FilterSelect = ({ label, value, options, onChange }) => (
  <div style={{ display: 'flex', flexDirection: 'column', gap: 3, flex: 1, minWidth: 150 }}>
    <label style={{ fontSize: 10, fontWeight: 700, color: '#323130', textTransform: 'uppercase', letterSpacing: .4 }}>{label}</label>
    <select value={value} onChange={e => onChange(e.target.value)} style={{ fontSize: 12, padding: '5px 8px', borderRadius: 4, width: '100%', border: value && value !== 'ALL' ? `1.5px solid ${C.amber}` : `1px solid ${C.border}`, background: value && value !== 'ALL' ? C.amberLight : C.card, color: '#323130', outline: 'none', cursor: 'pointer', fontWeight: value && value !== 'ALL' ? 700 : 'normal' }}>
      {(Array.isArray(options) ? options : ['ALL']).map(o => (<option key={o} value={o}>{o}</option>))}
    </select>
  </div>
);

/* ─── CHART CARD ──────────────────────────────────────────────────────────────── */
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
  // Default to bar-vertical
  return (
    <div style={{ flex: 1, display: 'flex', alignItems: 'flex-end', gap: 8, marginTop: 10 }}>
      {[35, 65, 40, 80, 55, 90].map((h, i) => (
        <div key={i} className="chart-skeleton-bar" style={{ flex: 1, height: `${h}%` }} />
      ))}
    </div>
  );
};

const ChartCard = React.memo(({ title, subtitle, children, highlighted, onClear, style = {}, loading = false, skeletonType = 'bar-vertical' }) => (
  <div style={{ background: C.card, borderRadius: 8, padding: '12px 14px', border: highlighted ? `1.5px solid ${C.selectedBorder}` : `1px solid ${C.border}`, boxShadow: '0 2px 8px rgba(0,0,0,.05)', transition: 'all .2s', display: 'flex', flexDirection: 'column', ...style }}>
    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 8 }}>
      <div>
        <div style={{ fontSize: 13, fontWeight: 700, color: '#323130' }}>{title}</div>
        {subtitle && <div style={{ fontSize: 10, color: '#8A8886', marginTop: 2 }}>{subtitle}</div>}
      </div>
      {highlighted && onClear && (<button onClick={onClear} style={{ fontSize: 11, color: '#fff', background: C.amber, border: 'none', borderRadius: 4, padding: '3px 9px', cursor: 'pointer', fontWeight: 600, flexShrink: 0 }}>Clear</button>)}
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

/* ─── CASE TABLE ──────────────────────────────────────────────────────────────── */
const CaseTable = React.memo(({ data, events, onSelect, selectedId }) => {
  if (selectedId !== 'ALL' && selectedId != null) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', height: '320px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8, paddingBottom: 8, borderBottom: `1px solid ${C.border}` }}>
          <div><span style={{ fontSize: 11, color: C.slate }}>Event Log for Order: </span><strong style={{ fontSize: 13, color: '#323130' }}>{selectedId}</strong></div>
          <button onClick={() => onSelect('ALL')} style={{ fontSize: 11, background: C.amber, color: '#fff', border: 'none', padding: '4px 12px', borderRadius: 4, cursor: 'pointer', fontWeight: 600 }}>Back to List</button>
        </div>
        <div style={{ overflowY: 'auto', flex: 1 }}>
          {(!events || events.length === 0) ? <Empty /> : (
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '11px' }}>
              <thead style={{ position: 'sticky', top: 0, background: '#F0F2F5', zIndex: 1 }}>
                <tr>
                  <th style={{ padding: '8px', textAlign: 'left', borderBottom: `1px solid ${C.border}`, color: C.slate }}>Activity</th>
                  <th style={{ padding: '8px', textAlign: 'left', borderBottom: `1px solid ${C.border}`, color: C.slate }}>Timestamp</th>
                  <th style={{ padding: '8px', textAlign: 'left', borderBottom: `1px solid ${C.border}`, color: C.slate }}>User</th>
                </tr>
              </thead>
              <tbody>
                {events.map((e, i) => (<tr key={i} style={{ borderBottom: `1px solid ${C.border}`, background: i % 2 === 0 ? '#fff' : '#fafafa' }}><td style={{ padding: '6px 8px', color: '#323130', fontWeight: 600 }}>{e.Activity}</td><td style={{ padding: '6px 8px', color: '#605E5C' }}>{e.Timestamp}</td><td style={{ padding: '6px 8px', color: '#605E5C' }}>{e.User}</td></tr>))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    );
  }
  if (!Array.isArray(data) || !data.length) return <Empty />;
  return (
    <div style={{ overflowX: 'auto', height: '320px', overflowY: 'auto' }}>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '11px' }}>
        <thead style={{ position: 'sticky', top: 0, background: '#F0F2F5', zIndex: 1 }}>
          <tr>
            <th style={{ padding: '8px', textAlign: 'left', borderBottom: `1px solid ${C.border}`, color: C.slate }}>Order (AUFNR)</th>
            <th style={{ padding: '8px', textAlign: 'left', borderBottom: `1px solid ${C.border}`, color: C.slate }}>Item (POSNR)</th>
            <th style={{ padding: '8px', textAlign: 'left', borderBottom: `1px solid ${C.border}`, color: C.slate }}>Case ID</th>
            <th style={{ padding: '8px', textAlign: 'left', borderBottom: `1px solid ${C.border}`, color: C.slate }}>Start Date</th>
            <th style={{ padding: '8px', textAlign: 'left', borderBottom: `1px solid ${C.border}`, color: C.slate }}>End Date</th>
          </tr>
        </thead>
        <tbody>
          {data.map((row, i) => (<tr key={i} onClick={() => onSelect(row.case_id)} style={{ borderBottom: `1px solid ${C.border}`, background: row.case_id === selectedId ? C.amberLight : (i % 2 === 0 ? '#fff' : '#fafafa'), cursor: 'pointer', borderLeft: row.case_id === selectedId ? `4px solid ${C.amber}` : '4px solid transparent' }}>
            <td style={{ padding: '6px 8px', color: '#323130', fontWeight: 600 }}>{row.aufnr || row.case_id}</td>
            <td style={{ padding: '6px 8px', color: '#605E5C' }}>{row.posnr || '—'}</td>
            <td style={{ padding: '6px 8px', color: '#8A8886', fontSize: 10, fontFamily: 'monospace' }}>{row.case_id}</td>
            <td style={{ padding: '6px 8px', color: '#605E5C' }}>{row.start_date}</td>
            <td style={{ padding: '6px 8px', color: '#605E5C' }}>{row.end_date}</td>
          </tr>))}
        </tbody>
      </table>
    </div>
  );
});

/* ─── CHARTS ─────────────────────────────────────────────────────────────────── */
const ActivityChart = React.memo(({ data, crossFilter, onSelect, isAnimationActive = true }) => {
  if (!Array.isArray(data) || !data.length) return <EmptyState condition={true} message="Data Not Uploaded / Available" />;
  const af = crossFilter?.type === 'activity' ? crossFilter.value : null;
  const rows = data.slice(0, 10);
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
      <div style={{ display: 'flex', gap: 14 }}>{[['#D97706', 'Events'], ['#92400E', 'Unique Cases']].map(([c, l]) => (<div key={l} style={{ display: 'flex', alignItems: 'center', gap: 4 }}><div style={{ width: 10, height: 10, borderRadius: 2, background: c }} /><span style={{ fontSize: 10, color: C.slate }}>{l}</span></div>))}</div>
      <div style={{ width: '100%', height: 280 }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={rows} layout="vertical" margin={{ left: 40, right: 20, top: 4, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" horizontal={false} />
            <XAxis type="number" hide />
            <YAxis type="category" dataKey="activity" tick={{ fontSize: 10, fill: '#605E5C' }} width={160} interval={0} />
            <Tooltip cursor={{ fill: 'rgba(0,0,0,.03)' }} content={<CustomTooltip nameKey="activity" />} />
            <Bar dataKey="count" barSize={18} onClick={e => e?.activity && onSelect('activity', e.activity === af ? null : e.activity)} isAnimationActive={isAnimationActive}>
              {rows.map((e, i) => (<Cell key={i} cursor="pointer" fill={af === e?.activity ? '#005A9E' : C.amber} opacity={af && af !== e?.activity ? 0.35 : 1} />))}
            </Bar>
            <Bar dataKey="unique_cases" radius={[0, 3, 3, 0]} barSize={18} onClick={e => e?.activity && onSelect('activity', e.activity === af ? null : e.activity)} isAnimationActive={isAnimationActive}>
              {rows.map((e, i) => (<Cell key={i} cursor="pointer" fill={af === e?.activity ? '#999999' : '#92400E'} opacity={af && af !== e?.activity ? 0.3 : 0.9} />))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
});

const MonthlyChart = React.memo(({ data, crossFilter, onSelect, isAnimationActive = true }) => {
  if (!Array.isArray(data) || !data.length) return <EmptyState condition={true} message="Data Not Uploaded / Available" />;
  return (
    <div style={{ width: '100%', height: 220 }}>
      <ResponsiveContainer width="100%" height="100%">
        <ComposedChart data={data} margin={{ left: 8, right: 8, top: 10, bottom: 40 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" />
          <XAxis dataKey="Month" tick={{ fontSize: 10, fill: '#605E5C' }} angle={-45} textAnchor="end" interval={0} />
          <YAxis tick={{ fontSize: 10, fill: '#605E5C' }} width={40} />
          <Tooltip content={<CustomTooltip nameKey="Month" labelOverride="cases" />} />
          <Bar dataKey="count" fill="transparent" cursor="pointer" onClick={e => e?.Month && onSelect('month', e.Month === crossFilter?.value ? null : e.Month)} isAnimationActive={isAnimationActive} />
          <Line type="monotone" dataKey="count" stroke={C.amber} strokeWidth={2.5} dot={{ r: 3, fill: C.amber }} activeDot={{ r: 6, fill: C.orange, stroke: '#fff', strokeWidth: 2, cursor: 'pointer', onClick: (e, payload) => { if (payload?.payload?.Month) onSelect('month', payload.payload.Month === crossFilter?.value ? null : payload.payload.Month); } }} isAnimationActive={isAnimationActive} />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
});

const DonutChart = React.memo(({ data, dataKey, nameKey, crossFilterType, crossFilter, onSelect, colors, isAnimationActive = true }) => {
  if (!Array.isArray(data) || !data.length) return <EmptyState condition={true} message="Data Not Uploaded / Available" />;
  const af = crossFilter?.type === crossFilterType ? crossFilter.value : null;
  const total = data.reduce((s, d) => s + (d[dataKey] || 0), 0);
  const chartData = data.map((d, i) => ({ ...d, fill: colors ? colors[i] : ACCENT[i % ACCENT.length] }));
  const renderLabel = ({ cx, cy, midAngle, innerRadius, outerRadius, percent }) => {
    if (percent < 0.05) return null;
    const R = Math.PI / 180, r = innerRadius + (outerRadius - innerRadius) * .55;
    const x = cx + r * Math.cos(-midAngle * R), y = cy + r * Math.sin(-midAngle * R);
    return <text x={x} y={y} fill="#fff" textAnchor="middle" dominantBaseline="central" style={{ fontSize: 10, fontWeight: 700, pointerEvents: 'none' }}>{`${(percent * 100).toFixed(0)}%`}</text>;
  };
  return (
    <div style={{ width: '100%', height: 260 }}>
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Pie data={chartData} dataKey={dataKey} nameKey={nameKey} cx="40%" cy="50%" innerRadius={60} outerRadius={95} paddingAngle={2} labelLine={false} label={renderLabel} onClick={e => e?.[nameKey] && onSelect(crossFilterType, e[nameKey] === af ? null : e[nameKey])} isAnimationActive={isAnimationActive}>
            {chartData.map((entry, i) => (<Cell key={i} fill={entry.fill} cursor="pointer" opacity={af && af !== entry?.[nameKey] ? 0.2 : 1} stroke={af === entry?.[nameKey] ? '#323130' : 'none'} strokeWidth={af === entry?.[nameKey] ? 2 : 0} />))}
          </Pie>
          <text x="37.5%" y="46%" textAnchor="middle" dominantBaseline="middle" style={{ fontSize: 18, fontWeight: 800, fill: '#323130' }}>{Number(af ? (chartData.find(d => d[nameKey] === af)?.[dataKey] || 0) : total).toLocaleString()}</text>
          <text x="37.5%" y="53%" textAnchor="middle" dominantBaseline="middle" style={{ fontSize: 10, fill: '#8A8886', fontWeight: 600 }}>{af || 'Total'}</text>
          <Tooltip formatter={v => [Number(v).toLocaleString(), 'Cases']} contentStyle={{ background: '#fff', border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 12 }} />
          <Legend layout="vertical" verticalAlign="middle" align="right" iconType="circle" wrapperStyle={{ fontSize: '11px', cursor: 'pointer', right: 10 }} onClick={entry => { if (entry?.value) onSelect(crossFilterType, entry.value === af ? null : entry.value); }} />
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
});

const ScrollableHBarChart = React.memo(({ data, dataKey, labelKey, crossFilter, crossKey, onSelect, color, isAnimationActive = true }) => {
  if (!Array.isArray(data) || !data.length) return <EmptyState condition={true} message="Data Not Uploaded / Available" />;
  const af = crossFilter?.type === crossKey ? crossFilter.value : null;
  const chartH = Math.max(220, data.length * 30);
  return (
    <div style={{ width: '100%', height: 220, overflowY: 'auto', paddingRight: 8 }}>
      <div style={{ height: chartH }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} layout="vertical" margin={{ left: 10, right: 20, top: 4, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" horizontal={false} />
            <XAxis type="number" hide />
            <YAxis type="category" dataKey={labelKey} tick={{ fontSize: 10, fill: '#605E5C' }} width={120} interval={0} />
            <Tooltip cursor={{ fill: 'rgba(0,0,0,.04)' }} content={<CustomTooltip nameKey={labelKey} labelOverride="cases" />} />
            <Bar dataKey={dataKey} radius={[0, 3, 3, 0]} barSize={20} onClick={e => e && e[labelKey] && onSelect(crossKey, e[labelKey] === af ? null : e[labelKey])} isAnimationActive={isAnimationActive}>
              {data.map((entry, i) => (<Cell key={i} cursor="pointer" fill={color || C.amber} opacity={af && af !== entry?.[labelKey] ? 0.25 : 1} />))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
});

const LeadTimeChart = React.memo(({ data, crossFilter, onSelect, isAnimationActive = true }) => {
  if (!Array.isArray(data) || !data.length) return <EmptyState condition={true} message="Data Not Uploaded / Available" />;
  const af = crossFilter?.type === 'lead_time' ? crossFilter.value : null;
  return (
    <div style={{ width: '100%', height: 235 }}>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ left: 8, right: 8, top: 8, bottom: 40 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" vertical={false} />
          <XAxis dataKey="label" tick={{ fontSize: 9, fill: '#605E5C' }} angle={-45} textAnchor="end" interval={1} />
          <YAxis tick={{ fontSize: 10, fill: '#605E5C' }} width={40} />
          <Tooltip content={<CustomTooltip nameKey="label" labelOverride="cases" />} />
          <Bar dataKey="count" radius={[2, 2, 0, 0]} onClick={e => e?.label && onSelect('lead_time', e.label === af ? null : e.label)} isAnimationActive={isAnimationActive}>
            {data.map((entry, i) => (<Cell key={i} cursor="pointer" fill={af === entry.label ? C.orange : C.amber} opacity={af && af !== entry.label ? 0.35 : 1} />))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
});

const BottleneckTooltip = ({ active, payload }) => {
  if (!active || !payload?.length) return null;
  const d = payload[0].payload;
  return (<div style={{ background: 'rgba(255,255,255,.98)', border: `1px solid #E1DFDD`, borderRadius: 6, padding: '8px 14px', fontSize: 12, color: '#323130' }}><div style={{ fontWeight: 700, color: C.amber, marginBottom: 4 }}>{d.step}</div><div style={{ color: '#605E5C' }}>Avg Days: <strong style={{ color: '#323130' }}>{d.avg_days}</strong></div><div style={{ color: '#605E5C' }}>Median Days: <strong style={{ color: C.teal }}>{d.median_days}</strong></div><div style={{ color: '#605E5C' }}>Cases: <strong>{Number(d.count).toLocaleString()}</strong></div></div>);
};

const BottleneckChart = React.memo(({ data, isAnimationActive = true }) => {
  if (!Array.isArray(data) || !data.length) return <Empty />;
  return (
    <div style={{ width: '100%', height: 240 }}>
      <ResponsiveContainer width="100%" height="110%">
        <ComposedChart data={data} margin={{ left: 8, right: 20, top: 8, bottom: 70 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" vertical={false} />
          <XAxis dataKey="step" tick={{ fontSize: 10, fill: '#605E5C' }} angle={-30} textAnchor="end" interval={0} />
          <YAxis tick={{ fontSize: 10, fill: '#605E5C' }} width={45} label={{ value: 'Days', angle: -90, position: 'insideLeft', offset: 5, style: { fontSize: 10, fill: '#605E5C' } }} />
          <Tooltip content={<BottleneckTooltip />} />
          <Bar dataKey="avg_days" name="Avg Days" radius={[4, 4, 0, 0]} fill={C.amber} isAnimationActive={isAnimationActive} />
          <Line type="monotone" dataKey="median_days" name="Median Days" stroke={C.orange} strokeWidth={2} dot={{ fill: C.orange, r: 4 }} isAnimationActive={isAnimationActive} />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
});

/* ─── P2I INTRO/FAQ SCREEN ───────────────────────────────────────────────────── */
const FaqItem = ({ q, a, bullets }) => {
  const [open, setOpen] = useState(false);
  return (
    <div style={{ borderBottom: '1px solid #E2E8F0' }}>
      <button onClick={() => setOpen(o => !o)} style={{ width: '100%', display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '14px 0', background: 'none', border: 'none', cursor: 'pointer', textAlign: 'left', gap: 12 }}>
        <span style={{ fontSize: 13.5, fontWeight: 600, color: '#1e293b', lineHeight: 1.4 }}>{q}</span>
        <span style={{ fontSize: 18, color: C.amber, flexShrink: 0, fontWeight: 700, transform: open ? 'rotate(45deg)' : 'none', transition: 'transform 0.2s', display: 'inline-block', width: 20, textAlign: 'center' }}>+</span>
      </button>
      {open && (
        <div style={{ paddingBottom: 16, fontSize: 13, color: '#475569', lineHeight: 1.75 }}>
          {a && <p style={{ margin: '0 0 8px' }}>{a}</p>}
          {bullets && bullets.length > 0 && (<ul style={{ margin: 0, paddingLeft: 20, display: 'flex', flexDirection: 'column', gap: 5 }}>{bullets.map((b, i) => (<li key={i} style={{ color: '#334155', lineHeight: 1.6 }}>{typeof b === 'object' && b.bold ? <><strong style={{ color: '#1e293b' }}>{b.bold}</strong>{b.rest}</> : b}</li>))}</ul>)}
        </div>
      )}
    </div>
  );
};

const P2I_FAQS = [
  { q: 'What is Plan-to-Inventory (P2I)?', a: 'P2I is the end-to-end manufacturing workflow from production planning to finished goods in inventory. It integrates production orders, material reservations, shop-floor confirmations, and goods movements to ensure efficient production execution.', bullets: [{ bold: 'Production Planning:', rest: ' Creating production orders with scheduled dates' }, { bold: 'Material Staging:', rest: ' Reserving components and issuing goods to the order' }, { bold: 'Shop Floor Execution:', rest: ' Confirming operations and recording actual times' }, { bold: 'Goods Receipt & Inspection:', rest: ' Posting finished goods into inventory and quality inspection' }, { bold: 'Procurement Link:', rest: ' Procuring raw materials via PR/PO/GR/Invoice' }] },
  { q: 'What SAP tables does P2I Process Mining use?', a: 'The P2I module draws from production and procurement SAP tables:', bullets: [{ bold: 'AUFK (Optional):', rest: ' Production Order Header — Creation date (ERDAT) and Order Creator (ERNAM) (highly recommended)' }, { bold: 'AFKO (Optional):', rest: ' Production Order Header Scheduling — Basic/Scheduled/Actual dates' }, { bold: 'AFPO:', rest: ' Production Order Items — material, plant, quantities (mandatory)' }, { bold: 'AFRU:', rest: ' Operation Confirmations — execution start, finish, and reversals' }, { bold: 'RESB:', rest: ' Production Reservations — component requirements' }, { bold: 'MKPF/MSEG:', rest: ' Material Documents — goods movements (261/101/131/262/102/132/541)' }, { bold: 'EKKO/EKPO/EBAN/EKBE:', rest: ' Standard P2P procurement chain' }] },
  { q: 'What are the key P2I activities tracked?', a: 'Activities span the full production lifecycle:', bullets: ['Production Order Creation (from AUFK)', 'Order Confirmation & Execution (from AFRU)', 'Goods Issued to Order (BWART 261) — materials consumed', 'Finished Goods Receipt (BWART 101) — completed products received', 'Finished Goods to Quality Inspection (BWART 131) — quality checks', 'Operation Reversed / Goods Issued Reversed / Goods transferred to subcontractor / Finished Goods Reversed / Quality Inspection to Finished Goods'] },
  { q: 'What is the P2I Happy Path?', a: 'The standard (Happy Path) P2I process follows this sequence without any reversals:', bullets: ['1. Production Order Creation', '2. Order Confirmation', '3. Basic Start', '4. Scheduled Start', '5. Actual Start', '6. Goods Issued to Order', '7. Finished Goods Receipt', '8. Finished Goods to Quality Inspection', 'Any deviation (reversal, sequence violation, missing step) is flagged as a process deviation.'] },
  { q: 'What KPIs does P2I Process Mining track?', a: 'Key metrics include:', bullets: [{ bold: 'Orders Confirmed:', rest: ' Orders with an Operation Confirmation' }, { bold: 'Goods Issued / FG Receipts:', rest: ' Completion of goods movements' }, { bold: 'GI Before Confirmation:', rest: ' Goods issued before order was confirmed (sequence violation)' }, { bold: 'FG Without GI:', rest: ' Finished goods received without goods issue (data gap)' }, { bold: 'Avg Completion Days:', rest: ' Creation Date to Quality Inspection finish' }] },
  { q: 'How does the Transformer work?', a: 'The P2I Transformer replicates the KNIME P2I_4 workflow in Python. Upload the raw SAP CSVs. AFPO is mandatory. Upload AUFK to get order creation date and creator (ERNAM). Upload AFKO to get scheduling details (Basic Start, etc.). All other tables add material movements and procurement activities. The transformer joins them, classifies movements by BWART, applies deletion-flag reversal rules, and outputs a single wide-format event log with one row per production order.', bullets: [] },
];

/* ─── TABLE UPLOAD SCREEN ────────────────────────────────────────────────────── */
const TableUploadScreen = ({ onBuilt, onBack, onLoadingChange, currentUser, myFiles, fetchingFiles, handleLoadOldFile }) => {
  const tables = [
    {
      name: 'AUFK', desc: 'Production Order Header (Optional)', isMandatory: false, required: [
        { col: 'AUFNR', note: 'Order number — join key' },
        { col: 'ERDAT', note: 'Production Order Creation Date' },
        { col: 'ERNAM', note: 'Order Creator User' }
      ]
    },
    {
      name: 'AFKO', desc: 'Production Order Header Scheduling (Optional)', isMandatory: false, required: [
        { col: 'AUFNR', note: 'Order number — join key to AFPO' },
        { col: 'GSTRP', note: 'Basic Start Date → "Basic Start"' },
        { col: 'GLTRP', note: 'Basic Finish Date → "Basic Finish"' },
        { col: 'FTRMS', note: 'Scheduled Release Date' },
        { col: 'GETRI', note: 'Confirmed Order Finish Date' },
        { col: 'GSTRI', note: 'Actual Start Date' },
        { col: 'GLTRI', note: 'Actual Finish Date' },
        { col: 'GLTRS', note: 'Scheduled Finish Date' },
        { col: 'GSTRS', note: 'Scheduled Start Date' },
        { col: 'FTRMI', note: 'Actual Release Date' },
        { col: 'FTRMP', note: 'Planned Release Date' }
      ]
    },
    {
      name: 'AFPO', desc: 'Production Order Items', isMandatory: true, required: [
        { col: 'AUFNR', note: 'Order number - join key' },
        { col: 'POSNR', note: 'Order Item' },
        { col: 'MATNR', note: 'Material Number' },
        { col: 'WERKS', note: 'Plant' },
        { col: 'MEINS', note: 'Base Unit of Measure' },
        { col: 'XLOEK', note: 'Deletion Flag' }
      ]
    },
    {
      name: 'AFRU', desc: 'Operation Confirmations', required: [
        { col: 'AUFNR', note: 'Order number - join key' },
        { col: 'ERSDA', note: 'Confirmation Date' },
        { col: 'ERNAM', note: 'User' },
        { col: 'BUDAT', note: 'Posting Date' },
        { col: 'STOKZ', note: 'Reversal indicator' },
        { col: 'ISDD', note: 'Actual Start Date' },
        { col: 'IEDD', note: 'Actual Finish Date' },
        { col: 'ISBD', note: 'Actual Start Time' }
      ]
    },
    {
      name: 'RESB', desc: 'Production Reservations', required: [
        { col: 'AUFNR', note: 'Order number - join key' },
        { col: 'RSNUM', note: 'Reservation number' },
        { col: 'RSPOS', note: 'Reservation item' },
        { col: 'MATNR', note: 'Material Number' },
        { col: 'WERKS', note: 'Plant' },
        { col: 'BDMNG', note: 'Requirement Quantity' }
      ]
    },
    {
      name: 'MKPF', desc: 'Material Doc Headers', required: [
        { col: 'MBLNR', note: 'Material Document' },
        { col: 'MJAHR', note: 'Material Doc Year' },
        { col: 'BUDAT', note: 'Posting Date' },
        { col: 'USNAM', note: 'User' },
        { col: 'TCODE', note: 'Transaction Code' }
      ]
    },
    {
      name: 'MSEG', desc: 'Material Doc Lines', required: [
        { col: 'MBLNR', note: 'Material Document - join key' },
        { col: 'MJAHR', note: 'Material Doc Year - join key' },
        { col: 'AUFNR', note: 'Order number' },
        { col: 'BWART', note: 'Movement Type (261, 101, 262, 102)' },
        { col: 'BUDAT', note: 'Posting Date' },
        { col: 'EBELN', note: 'Purchase Order (if procured)' },
        { col: 'EBELP', note: 'PO Item' }
      ]
    },
    {
      name: 'EKKO', desc: 'PO Header', required: [
        { col: 'EBELN', note: 'PO number' },
        { col: 'AEDAT', note: 'Creation date' },
        { col: 'BEDAT', note: 'Document date' },
        { col: 'BSART', note: 'Document type' },
        { col: 'LIFNR', note: 'Vendor' },
        { col: 'BUKRS', note: 'Company code' },
        { col: 'ERNAM', note: 'Creator' }
      ]
    },
    {
      name: 'EKPO', desc: 'PO Items', required: [
        { col: 'EBELN', note: 'PO number' },
        { col: 'EBELP', note: 'PO Item' },
        { col: 'MATNR', note: 'Material Number' },
        { col: 'WERKS', note: 'Plant' },
        { col: 'MATKL', note: 'Material Group' },
        { col: 'BANFN', note: 'PR number' },
        { col: 'BNFPO', note: 'PR item' },
        { col: 'LOEKZ', note: 'Deletion Flag' },
        { col: 'AEDAT', note: 'Creation Date' }
      ]
    },
    {
      name: 'EBAN', desc: 'Purchase Requisitions', required: [
        { col: 'BANFN', note: 'PR number' },
        { col: 'BNFPO', note: 'PR item' },
        { col: 'BADAT', note: 'Requisition date' },
        { col: 'FRGDT', note: 'Release date' },
        { col: 'ERNAM', note: 'Creator' },
        { col: 'ERDAT', note: 'Creation date' },
        { col: 'LOEKZ', note: 'Deletion Flag' }
      ]
    },
    {
      name: 'EKBE', desc: 'PO History / GR & Invoice', required: [
        { col: 'EBELN', note: 'PO number' },
        { col: 'EBELP', note: 'PO item' },
        { col: 'VGABE', note: 'Transaction type (1=GR, 2=IR)' },
        { col: 'BUDAT', note: 'Posting Date' },
        { col: 'SHKZG', note: 'Debit/Credit (reversals)' },
        { col: 'ERNAM', note: 'User' }
      ]
    },
    {
      name: 'LFA1', desc: 'Vendor Master', required: [
        { col: 'LIFNR', note: 'Vendor ID' },
        { col: 'NAME1', note: 'Vendor Name' }
      ]
    }
  ];

  const [tableStatus, setTableStatus] = useState(Object.fromEntries(tables.map(t => [t.name, 'idle'])));
  const [tableMsg, setTableMsg] = useState(Object.fromEntries(tables.map(t => [t.name, ''])));
  const [building, setBuilding] = useState(false);
  const [buildMsg, setBuildMsg] = useState('');
  const [colMapping, setColMapping] = useState(null); // {tableName, file, tableDef, uploadedCols:[], mapping:{}}
  const [tableCols, setTableCols] = useState({});
  const [selectedFiles, setSelectedFiles] = useState({});
  const [appliedMappings, setAppliedMappings] = useState({});

  const fileRefs = useRef(Object.fromEntries(tables.map(t => [t.name, React.createRef()])));

  const allDone = tables.filter(t => t.isMandatory).every(t => tableStatus[t.name] === 'done');
  const anyUploading = tables.some(t => tableStatus[t.name] === 'uploading') || building;

  // ── Clear server-side tables on mount so user always starts fresh ──────────
  React.useEffect(() => {
    fetch(`${API}/p2i/transform/status?username=${encodeURIComponent(currentUser || 'Unknown')}`)
      .then(r => r.ok ? r.json() : null)
      .then(d => {
        if (!d || !d.loaded) return;
        d.loaded.forEach(tName => {
          fetch(`${API}/p2i/transform/clear_table?table_name=${tName}&username=${encodeURIComponent(currentUser || 'Unknown')}`, { method: 'DELETE' }).catch(() => { });
        });
      }).catch(() => { });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const uploadTable = async (tableName, file) => {
    if (!file) return;
    if (!file.name.toLowerCase().endsWith('.csv')) {
      setTableStatus(p => ({ ...p, [tableName]: 'error' }));
      setTableMsg(p => ({ ...p, [tableName]: 'Only .csv accepted.' }));
      return;
    }
    setSelectedFiles(p => ({ ...p, [tableName]: file }));
    performUpload(tableName, file, {});
  };

  const handleMapColumns = async (tableName) => {
    const file = selectedFiles[tableName];
    if (!file) return;
    const formPreview = new FormData();
    formPreview.append('file', file);
    try {
      const rPrev = await fetch(`${API}/p2i/transform/preview_columns`, { method: 'POST', body: formPreview });
      const dPrev = await rPrev.json();
      if (!rPrev.ok) throw new Error(dPrev.detail || `Failed to read CSV columns`);
      const tDef = tables.find(t => t.name === tableName);
      setColMapping({ tableName, file, tableDef: tDef, uploadedCols: dPrev.columns, mapping: {} });
    } catch (e) {
      setTableStatus(p => ({ ...p, [tableName]: 'error' }));
      setTableMsg(p => ({ ...p, [tableName]: e.message }));
    }
  };

  const performUpload = async (tableName, file, mapping) => {
    setTableStatus(p => ({ ...p, [tableName]: 'uploading' }));
    setTableMsg(p => ({ ...p, [tableName]: '' }));
    const form = new FormData();
    form.append('file', file); form.append('table_name', tableName); form.append('username', currentUser || 'Unknown');
    form.append('column_mapping', JSON.stringify(mapping));
    try {
      const r = await fetch(`${API}/p2i/transform/upload_table`, { method: 'POST', body: form });
      const d = await r.json();
      if (!r.ok) throw new Error(d.detail || `HTTP ${r.status}`);
      setTableStatus(p => ({ ...p, [tableName]: 'done' }));
      setTableMsg(p => ({ ...p, [tableName]: `${Number(d.rows).toLocaleString()} rows` }));
      if (d.columns) {
        setTableCols(p => ({ ...p, [tableName]: d.columns }));
      }
      setColMapping(null);
    } catch (e) {
      setTableStatus(p => ({ ...p, [tableName]: 'error' }));
      setTableMsg(p => ({ ...p, [tableName]: e.message }));
    }
  };

  const handleBuild = async () => {
    setBuilding(true); setBuildMsg('');
    onLoadingChange && onLoadingChange(true, 20, 'Processing Data...');
    let prog = 20;
    const ticker = setInterval(() => { prog = Math.min(prog + Math.random() * 14, 88); onLoadingChange && onLoadingChange(true, prog, 'Analysing Data...'); }, 400);
    try {
      const r = await fetch(`${API}/p2i/transform/build?username=${encodeURIComponent(currentUser || 'Unknown')}`, { method: 'POST' });
      const d = await r.json();
      clearInterval(ticker);
      if (!r.ok) throw new Error(d.detail || `HTTP ${r.status}`);
      onLoadingChange && onLoadingChange(true, 100, 'Dashboard Created');
      setTimeout(() => {
        setBuilding(false);
        setBuildMsg(`✓ Success: ${Number(d.rows).toLocaleString()} rows processed`);
        onBuilt && onBuilt(null, 'table');
      }, 800);
    } catch (e) {
      clearInterval(ticker);
      onLoadingChange && onLoadingChange(false, 0, '');
      setBuilding(false);

      const isMappingErr = e.message.includes('Column mapping is incorrect');
      const match = e.message.match(/incorrect for (\w+)/);
      const tableName = match ? match[1] : null;
      if (tableName) {
        setTableStatus(p => ({ ...p, [tableName]: 'error' }));
        setTableMsg(p => ({ ...p, [tableName]: e.message }));
      }

      onBuilt && onBuilt(e.message, 'table');
    }
  };

  const handleClearAll = async () => {
    if (!window.confirm("Are you sure you want to clear all uploaded tables?")) return;
    try {
      const loadedTables = Object.keys(tableStatus).filter(t => tableStatus[t] === 'done' || tableStatus[t] === 'error');
      for (const tName of loadedTables) {
        await fetch(`${API}/p2i/transform/clear_table?table_name=${tName}&username=${encodeURIComponent(currentUser || 'Unknown')}`, { method: 'DELETE' });
      }
      setTableStatus(Object.fromEntries(tables.map(t => [t.name, 'idle'])));
      setTableMsg(Object.fromEntries(tables.map(t => [t.name, ''])));
      setSelectedFiles({});
      setAppliedMappings({});
      setBuildMsg('All tables cleared.');
    } catch (e) {
      setBuildMsg('Failed to clear some tables.');
    }
  };

  const si = (s) => {
    if (s === 'done') return { icon: '✓', color: '#107C10', bg: '#F0FAF0', border: '#107C10' };
    if (s === 'error') return { icon: '✕', color: '#D13438', bg: '#FDE7E9', border: '#D13438' };
    if (s === 'uploading') return { icon: '…', color: C.amber, bg: C.amberLight, border: C.amber };
    return { icon: '↑', color: C.amber, bg: '#fff', border: C.amber };
  };

  const tableBuilds = (myFiles || []).filter(f => f.source === 'table_build');

  return (
    <div style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', padding: '28px 24px 48px', overflowY: 'auto' }}>

      {/* ══ Column Mapping Modal ══════════════════════════════════════════════ */}
      {colMapping && (
        <div style={{
          position: 'fixed', inset: 0, zIndex: 9999, background: 'rgba(0,0,0,0.5)',
          display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 16
        }}
          onClick={() => setColMapping(null)}>
          <div style={{
            background: '#fff', borderRadius: 12, width: '100%', maxWidth: 700,
            maxHeight: '88vh', display: 'flex', flexDirection: 'column',
            boxShadow: '0 24px 64px rgba(0,0,0,0.35)'
          }}
            onClick={e => e.stopPropagation()}>
            <div style={{ padding: '20px 24px 16px', borderBottom: '1px solid #E2E8F0', flexShrink: 0 }}>
              <div style={{ fontSize: 10, fontWeight: 700, color: '#DC2626', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 4 }}>Column Mapping</div>
              <div style={{ fontSize: 17, fontWeight: 700, color: '#1e293b' }}>Map Columns for {colMapping.tableDef.name}</div>
              <div style={{ fontSize: 12, color: '#64748b', marginTop: 3 }}> Select which columns from your file correspond to the required fields.</div>
            </div>

            <div style={{ overflowY: 'auto', flex: 1, padding: '0 0 8px' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                <thead style={{ position: 'sticky', top: 0, zIndex: 2 }}>
                  <tr style={{ background: '#F8FAFC' }}>
                    <th style={{ padding: '10px 16px', textAlign: 'left', fontWeight: 700, color: '#64748b', fontSize: 10, textTransform: 'uppercase', letterSpacing: 0.8, borderBottom: '2px solid #E2E8F0', width: '35%' }}>Required Column</th>
                    <th style={{ padding: '10px 12px', textAlign: 'left', fontWeight: 700, color: '#64748b', fontSize: 10, textTransform: 'uppercase', letterSpacing: 0.8, borderBottom: '2px solid #E2E8F0', width: '35%' }}>Map to File Column</th>
                    <th style={{ padding: '10px 12px', textAlign: 'left', fontWeight: 700, color: '#64748b', fontSize: 10, textTransform: 'uppercase', letterSpacing: 0.8, borderBottom: '2px solid #E2E8F0' }}>Purpose</th>
                  </tr>
                </thead>
                <tbody>
                  {colMapping.tableDef.required.map((r, i) => {
                    const reqCol = r.col;
                    const autoMatch = colMapping.uploadedCols.find(c => c.toUpperCase() === reqCol.toUpperCase());
                    const selected = colMapping.mapping[reqCol] !== undefined ? colMapping.mapping[reqCol] : (autoMatch || '');
                    return (
                      <tr key={reqCol} style={{ borderBottom: '1px solid #F1F5F9', background: '#fff' }}>
                        <td style={{ padding: '10px 16px', fontFamily: 'monospace', fontWeight: 700, color: '#334155', fontSize: 13 }}>{reqCol}</td>
                        <td style={{ padding: '10px 12px' }}>
                          <select
                            value={selected}
                            onChange={e => setColMapping(p => ({ ...p, mapping: { ...p.mapping, [reqCol]: e.target.value } }))}
                            style={{ width: '100%', padding: '6px 8px', borderRadius: 4, border: '1px solid #CBD5E1', background: '#fff', fontSize: 12, color: '#334155' }}
                          >
                            <option value="">-- Leave Blank / Unmapped --</option>
                            {colMapping.uploadedCols.map(c => (
                              <option key={c} value={c}>{c}</option>
                            ))}
                          </select>
                        </td>
                        <td style={{ padding: '10px 12px', color: '#64748b', fontSize: 11, lineHeight: 1.4 }}>{r.note}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            <div style={{ padding: '14px 20px', borderTop: '1px solid #E2E8F0', flexShrink: 0, display: 'flex', justifyContent: 'flex-end', gap: 12 }}>
              <button onClick={() => setColMapping(null)}
                style={{ padding: '8px 16px', background: '#fff', color: '#64748b', border: '1px solid #CBD5E1', borderRadius: 6, fontSize: 13, fontWeight: 700, cursor: 'pointer' }}>
                Cancel
              </button>
              <button
                onClick={async () => {
                  const finalMapping = {};
                  colMapping.tableDef.required.forEach(r => {
                    const autoMatch = colMapping.uploadedCols.find(c => c.toUpperCase() === r.col.toUpperCase());
                    const sel = colMapping.mapping[r.col] !== undefined ? colMapping.mapping[r.col] : (autoMatch || '');
                    if (sel) {
                      finalMapping[sel] = r.col;
                    }
                  });
                  await fetch(`${API}/p2i/transform/clear_table?table_name=${colMapping.tableDef.name}&username=${encodeURIComponent(currentUser || 'Unknown')}`, { method: 'DELETE' }).catch(console.error);
                  setAppliedMappings(p => ({ ...p, [colMapping.tableDef.name]: finalMapping }));
                  performUpload(colMapping.tableDef.name, colMapping.file, finalMapping);
                }}
                style={{ padding: '8px 16px', background: C.amber, color: '#fff', border: 'none', borderRadius: 6, fontSize: 13, fontWeight: 700, cursor: 'pointer' }}
                onMouseOver={e => e.currentTarget.style.background = C.amberDark}
                onMouseOut={e => e.currentTarget.style.background = C.amber}>
                Confirm Mapping & Upload
              </button>
            </div>
          </div>
        </div>
      )}

      <div style={{ maxWidth: 820, width: '100%', display: 'flex', flexDirection: 'column', gap: 20 }}>
        {/* Back + header */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <button onClick={onBack}
            style={{
              background: 'none', border: '1px solid #E2E8F0', padding: '6px 14px', borderRadius: 6,
              fontSize: 12, cursor: 'pointer', color: '#64748b', fontWeight: 600
            }}
            onMouseOver={e => e.currentTarget.style.background = '#F8FAFC'}
            onMouseOut={e => e.currentTarget.style.background = 'none'}>
            ← Back
          </button>
          <div>
            <div style={{ fontSize: 11, fontWeight: 700, color: C.amber, textTransform: 'uppercase', letterSpacing: 0.8 }}>Build Event Log</div>
            <div style={{ fontSize: 13, color: '#64748b' }}>Upload SAP tables below, then click Build. AFPO is mandatory; AFKO is optional but recommended for production dates.</div>
          </div>
        </div>

        {/* Table upload panel */}
        <div style={{ background: '#fff', border: '1px solid #E2E8F0', borderRadius: 10, padding: '20px 22px', boxShadow: '0 2px 6px rgba(0,0,0,0.04)' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 14 }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: 0.8 }}>SAP Tables</div>
            <div style={{ fontSize: 11, color: '#94a3b8' }}>Upload each as <strong style={{ color: '#475569' }}>.csv</strong></div>
          </div>
          <motion.div
            variants={{
              visible: { transition: { staggerChildren: 0.05 } }
            }}
            initial="hidden"
            animate="visible"
            style={{ display: 'flex', flexDirection: 'column', border: '1px solid #E2E8F0', borderRadius: 8, overflow: 'hidden' }}
          >
            {tables.map((t, i) => {
              const s = si(tableStatus[t.name]);
              const ref = fileRefs.current[t.name];
              const isUp = tableStatus[t.name] === 'uploading';
              return (
                <motion.div
                  key={t.name}
                  variants={{
                    hidden: { opacity: 0, x: -10 },
                    visible: { opacity: 1, x: 0 }
                  }}
                  animate={isUp ? {
                    backgroundColor: ['#F8FAFC', C.amberLight, '#F8FAFC'],
                    transition: { duration: 1.5, repeat: Infinity }
                  } : {}}
                  style={{
                    display: 'flex', alignItems: 'center', gap: 12, padding: '10px 14px',
                    background: tableStatus[t.name] === 'done' ? '#F0FAF0' : tableStatus[t.name] === 'error' ? '#FDE7E9' : i % 2 === 0 ? '#F8FAFC' : '#fff',
                    borderBottom: i < tables.length - 1 ? '1px solid #E2E8F0' : 'none', transition: 'background 0.2s'
                  }}>
                  <input ref={ref} type="file" accept=".csv" style={{ display: 'none' }}
                    onChange={e => {
                      const f = e.target.files[0]; e.target.value = '';
                      if (f) uploadTable(t.name, f);
                    }} />
                  <button onClick={() => { if (!isUp && ref.current) { ref.current.value = ''; ref.current.click(); } }}
                    disabled={isUp}
                    style={{
                      display: 'flex', alignItems: 'center', justifyContent: 'center', width: 28, height: 28,
                      borderRadius: 6, border: `1.5px solid ${s.border}`, background: s.bg, color: s.color,
                      cursor: isUp ? 'not-allowed' : 'pointer', fontWeight: 700, fontSize: 13, flexShrink: 0
                    }}
                    onMouseOver={e => { if (!isUp) e.currentTarget.style.background = C.amberLight; }}
                    onMouseOut={e => { e.currentTarget.style.background = s.bg; }}>
                    {isUp ? <span style={{ animation: 'spin 1s linear infinite', display: 'inline-block' }}>↻</span> : s.icon}
                  </button>
                  <div style={{
                    minWidth: 52, fontFamily: 'monospace', fontWeight: 700, fontSize: 13, color: C.amberDark,
                    background: C.amberLight, padding: '3px 8px', borderRadius: 4, textAlign: 'center', flexShrink: 0
                  }}>{t.name}</div>
                  <div style={{ fontSize: 13, color: '#475569', flex: 1 }}>
                    {t.desc}
                    {appliedMappings[t.name] && Object.keys(appliedMappings[t.name]).length > 0 && (
                      <div style={{ fontSize: 11, color: '#006B3C', marginTop: 4, display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                        {Object.entries(appliedMappings[t.name]).map(([k, v]) => (
                          <span key={k} style={{ background: '#E6F4EA', padding: '2px 6px', borderRadius: 4 }}><strong>{k}</strong> → {v}</span>
                        ))}
                      </div>
                    )}
                  </div>
                  {/* Server-returned message (error or success) + clear button */}
                  <div style={{ display: 'flex', alignItems: 'center', gap: 5, marginLeft: 'auto', flexShrink: 0 }}>
                    {tableMsg[t.name] && (
                      <div style={{
                        fontSize: 11, fontWeight: 600, maxWidth: 260, lineHeight: 1.3,
                        color: tableStatus[t.name] === 'error' ? '#DC2626' : '#15803D',
                        background: tableStatus[t.name] === 'error' ? '#FEF2F2' : 'transparent',
                        padding: tableStatus[t.name] === 'error' ? '3px 6px' : '0',
                        borderRadius: 4, border: tableStatus[t.name] === 'error' ? '1px solid #FECACA' : 'none'
                      }}>
                        {tableMsg[t.name]}
                      </div>
                    )}
                    {(tableStatus[t.name] === 'done' || tableStatus[t.name] === 'error') && (
                      <div style={{ display: 'flex', gap: 4, alignItems: 'center' }}>
                        {selectedFiles[t.name] && (
                          <button
                            onClick={() => handleMapColumns(t.name)}
                            title='Map columns'
                            style={{
                              fontSize: 10, fontWeight: 700, padding: '2px 8px', borderRadius: 4,
                              background: tableStatus[t.name] === 'error' ? '#FEF2F2' : C.amberLight,
                              color: tableStatus[t.name] === 'error' ? '#DC2626' : C.amberDark,
                              border: tableStatus[t.name] === 'error' ? '1px solid #FCA5A5' : `1px solid ${C.amber}`,
                              cursor: 'pointer'
                            }}
                            onMouseOver={e => e.currentTarget.style.background = tableStatus[t.name] === 'error' ? '#FECACA' : C.amberLight}
                            onMouseOut={e => e.currentTarget.style.background = tableStatus[t.name] === 'error' ? '#FEF2F2' : C.amberLight}>
                            Map Columns
                          </button>
                        )}
                        <button
                          onClick={() => {
                            fetch(`${API}/p2i/transform/clear_table?table_name=${t.name}&username=${encodeURIComponent(currentUser || 'Unknown')}`, { method: 'DELETE' }).catch(console.error);
                            setTableStatus(p => ({ ...p, [t.name]: 'idle' }));
                            setTableMsg(p => ({ ...p, [t.name]: '' }));
                            setSelectedFiles(p => { const copy = { ...p }; delete copy[t.name]; return copy; });
                            setAppliedMappings(p => { const copy = { ...p }; delete copy[t.name]; return copy; });
                            if (fileRefs.current[t.name]?.current) fileRefs.current[t.name].current.value = '';
                          }}
                          title='Clear to re-upload'
                          style={{
                            display: 'flex', alignItems: 'center', justifyContent: 'center',
                            width: 18, height: 18, borderRadius: '50%', border: '1.5px solid #FCA5A5',
                            background: '#FEE2E2', color: '#DC2626', cursor: 'pointer',
                            fontWeight: 800, fontSize: 10, padding: 0, lineHeight: 1, flexShrink: 0
                          }}
                          onMouseOver={e => e.currentTarget.style.background = '#FECACA'}
                          onMouseOut={e => e.currentTarget.style.background = '#FEE2E2'}>
                          ✕
                        </button>
                      </div>
                    )}
                  </div>
                </motion.div>
              );
            })}
          </motion.div>
          <div style={{ marginTop: 16, display: 'flex', alignItems: 'center', gap: 12, justifyContent: 'space-between', flexWrap: 'wrap' }}>
            <div style={{ fontSize: 12, color: allDone ? '#107C10' : '#94a3b8', fontWeight: allDone ? 700 : 400 }}>
              {allDone ? `✓ ${tables.filter(t => tableStatus[t.name] === 'done').length} table(s) uploaded — ready to build` : `${tables.filter(t => tableStatus[t.name] === 'done').length} / ${tables.length} tables uploaded — AFPO mandatory, AFKO optional`}
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
              {buildMsg && <div style={{ fontSize: 12, color: buildMsg.startsWith('Error') ? '#D13438' : '#107C10', fontWeight: 600 }}>{buildMsg}</div>}
              {buildMsg && !buildMsg.startsWith('Error') && (
                <button onClick={() => { const url = `${API}/p2i/download_output?username=${encodeURIComponent(currentUser || 'Unknown')}`; const a = document.createElement('a'); a.href = url; a.download = ''; a.click(); }}
                  style={{ background: C.amber, color: '#fff', border: 'none', padding: '5px 14px', borderRadius: 4, fontSize: 12, fontWeight: 700, cursor: 'pointer' }}
                  onMouseOver={e => e.currentTarget.style.background = C.amberDark}
                  onMouseOut={e => e.currentTarget.style.background = C.amber}>
                  ⬇ Download CSV
                </button>
              )}
              <button onClick={handleClearAll} disabled={anyUploading}
                style={{
                  background: '#fff', color: '#D13438', border: '1px solid #D13438',
                  padding: '9px 18px', borderRadius: 6, fontSize: 13, fontWeight: 700,
                  cursor: anyUploading ? 'not-allowed' : 'pointer', whiteSpace: 'nowrap'
                }}
                onMouseOver={e => { if (!anyUploading) e.currentTarget.style.background = '#FDE7E9'; }}
                onMouseOut={e => { e.currentTarget.style.background = '#fff'; }}>
                Clear All
              </button>
              <button onClick={handleBuild} disabled={!allDone || anyUploading}
                style={{
                  background: allDone && !anyUploading ? C.amber : '#A8A8A8', color: '#fff', border: 'none',
                  padding: '10px 28px', borderRadius: 6, fontSize: 13, fontWeight: 700,
                  cursor: allDone && !anyUploading ? 'pointer' : 'not-allowed', whiteSpace: 'nowrap',
                  boxShadow: allDone && !anyUploading ? `0 2px 8px rgba(217,119,6,0.3)` : 'none'
                }}
                onMouseOver={e => { if (allDone && !anyUploading) e.currentTarget.style.background = C.amberDark; }}
                onMouseOut={e => { e.currentTarget.style.background = allDone && !anyUploading ? C.amber : '#A8A8A8'; }}>
                {building ? '⏳ Building…' : 'Build Event Log'}
              </button>
            </div>
          </div>
        </div>

        {/* Previous Table Builds */}
        <div style={{ background: '#fff', border: '1px solid #E2E8F0', borderRadius: 10, padding: '18px 20px', boxShadow: '0 2px 6px rgba(0,0,0,0.04)' }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: 0.8, marginBottom: 12 }}>Previous Table Builds</div>
          {fetchingFiles ? (
            <div style={{ color: '#94a3b8', fontSize: 13 }}>Loading...</div>
          ) : tableBuilds.length === 0 ? (
            <div style={{ padding: '20px', textAlign: 'center', background: '#F8FAFC', borderRadius: 8, border: '1px dashed #E2E8F0', color: '#94a3b8', fontSize: 13 }}>
              No previous builds found. Upload tables above and click Build.
            </div>
          ) : (
            <div style={{ border: '1px solid #E2E8F0', borderRadius: 8, overflow: 'hidden' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                <thead style={{ background: '#F3F2F1', borderBottom: '1px solid #E2E8F0', textAlign: 'left' }}>
                  <tr>
                    <th style={{ padding: '9px 14px', color: '#323130', fontWeight: 600 }}>Name</th>
                    <th style={{ padding: '9px 14px', color: '#323130', fontWeight: 600 }}>Date</th>
                    <th style={{ padding: '9px 14px', color: '#323130', fontWeight: 600, textAlign: 'right' }}>Cases</th>
                    <th style={{ padding: '9px 14px', color: '#323130', fontWeight: 600, textAlign: 'right' }}>Rows</th>
                    <th style={{ padding: '9px 14px', color: '#323130', fontWeight: 600 }}>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {tableBuilds.map((f, idx) => (
                    <tr key={idx} style={{ borderBottom: '1px solid #E2E8F0', transition: 'background 0.2s' }}
                      onMouseEnter={e => e.currentTarget.style.background = '#F8FAFC'}
                      onMouseLeave={e => e.currentTarget.style.background = 'transparent'}>
                      <td style={{ padding: '9px 14px' }}>
                        <div style={{ fontWeight: 600, color: '#1e293b', fontSize: 12 }}>{f.filename}</div>
                        <span style={{ fontSize: 9, fontWeight: 700, padding: '2px 7px', borderRadius: 10, background: C.amberLight, color: C.amberDark, border: `1px solid ${C.amber}` }}>Table Build</span>
                      </td>
                      <td style={{ padding: '9px 14px', color: '#64748b', whiteSpace: 'nowrap', fontSize: 11 }}>{f.upload_date}</td>
                      <td style={{ padding: '9px 14px', color: '#1e293b', fontWeight: 600, textAlign: 'right' }}>{f.cases != null ? Number(f.cases).toLocaleString() : '—'}</td>
                      <td style={{ padding: '9px 14px', color: '#64748b', textAlign: 'right' }}>{f.rows != null ? Number(f.rows).toLocaleString() : '—'}</td>
                      <td style={{ padding: '9px 14px' }}>
                        <button onClick={() => handleLoadOldFile && handleLoadOldFile(f.file_id)}
                          style={{ background: C.amber, color: '#fff', border: 'none', padding: '5px 12px', borderRadius: 4, cursor: 'pointer', fontSize: 11, fontWeight: 600 }}
                          onMouseOver={e => e.currentTarget.style.background = C.amberDark}
                          onMouseOut={e => e.currentTarget.style.background = C.amber}>
                          Load Dashboard
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

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

/* ─── P2I INTRO SCREEN ──────────────────────────────────────────────────────── */
const P2IIntroScreen = ({ onGoTableBuild, onGoCsvUpload, currentUser }) => {
  const [introStep, setIntroStep] = useState('overview'); // 'overview' | 'choose'
  const [hoveredSide, setHoveredSide] = useState(null);
  const [showFaqModal, setShowFaqModal] = useState(false);

  const steps = [
    'Order Confirmation & Operations',
    'Material Staging & Component Issuance',
    'Goods Issued to Order (Components)',
    'Finished Goods Receipts',
    'Production Reversals & Adjustments',
    'Final Inventory Posting'
  ];

  const kpis = [
    'Orders Confirmed Rate',
    'Goods Issued Completion %',
    'FG Receipt Accuracy',
    'Sequence Violation Rate',
    'Order Completion Cycle Time',
    'Production Reversal Rate'
  ];

  const icons = {
    file: (
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
        <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" /><polyline points="14 2 14 8 20 8" /><line x1="16" y1="13" x2="8" y2="13" /><line x1="16" y1="17" x2="8" y2="17" /><line x1="10" y1="9" x2="8" y2="9" />
      </svg>
    ),
    check: (
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
        <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14" /><polyline points="22 4 12 14.01 9 11.01" />
      </svg>
    ),
    cart: (
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
        <circle cx="9" cy="21" r="1" /><circle cx="20" cy="21" r="1" /><path d="M1 1h4l2.68 13.39a2 2 0 0 0 2 1.61h9.72a2 2 0 0 0 2-1.61L23 6H6" />
      </svg>
    ),
    truck: (
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
        <rect x="1" y="3" width="15" height="13" /><polygon points="16 8 20 8 23 11 23 16 16 16 16 8" /><circle cx="5.5" cy="18.5" r="2.5" /><circle cx="18.5" cy="18.5" r="2.5" />
      </svg>
    ),
    receipt: (
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
        <path d="M4 2v20l2-1 2 1 2-1 2 1 2-1 2 1 2-1 2 1V2l-2 1-2-1-2 1-2-1-2 1-2-1z" /><line x1="16" y1="8" x2="8" y2="8" /><line x1="16" y1="12" x2="8" y2="12" /><line x1="16" y1="16" x2="8" y2="16" />
      </svg>
    ),
    creditCard: (
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
        <rect x="1" y="4" width="22" height="16" rx="2" ry="2" /><line x1="1" y1="10" x2="23" y2="10" />
      </svg>
    )
  };

  const shortSteps = [
    { icon: icons.file, text: 'Order' },
    { icon: icons.check, text: 'Confirm' },
    { icon: icons.cart, text: 'GI' },
    { icon: icons.receipt, text: 'FG Receipt' },
    { icon: icons.truck, text: 'Inventory' },
    { icon: icons.creditCard, text: 'Adjustments' }
  ];

  const PremiumMetricAnimation = () => (
    <div style={{ position: 'relative', width: '60px', height: '60px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
      <motion.div animate={{ scale: [1, 1.6], opacity: [0.6, 0] }} transition={{ duration: 2, repeat: Infinity, ease: "easeOut" }} style={{ position: 'absolute', width: '40px', height: '40px', borderRadius: '50%', border: '2px solid rgba(255,255,255,0.8)' }} />
      <motion.div animate={{ scale: [1, 1.3], opacity: [0.4, 0] }} transition={{ duration: 2, repeat: Infinity, ease: "easeOut", delay: 0.5 }} style={{ position: 'absolute', width: '40px', height: '40px', borderRadius: '50%', border: '2px solid rgba(255,255,255,0.6)' }} />
      <motion.div animate={{ rotate: 360 }} transition={{ duration: 3, repeat: Infinity, ease: "linear" }} style={{ position: 'absolute', width: '32px', height: '32px', borderRadius: '50%', border: '2px solid transparent', borderTopColor: 'rgba(255,255,255,0.9)', borderRightColor: 'rgba(255,255,255,0.4)' }} />
      <motion.div animate={{ scale: [0.9, 1.1, 0.9] }} transition={{ duration: 2, repeat: Infinity, ease: "easeInOut" }} style={{ width: '10px', height: '10px', borderRadius: '50%', background: '#fff', boxShadow: '0 0 15px #fff, 0 0 30px #fff' }} />
    </div>
  );

  const KeyMetricsAnimation = ({ metrics }) => {
    const colors = ['#D97706', '#F7630C', '#CA5010', '#FF8C00', '#FFB900', '#A4262C'];
    const subtitles = [
      'Percent of production orders approved',
      'Percent of raw materials issued to shop floor',
      'Match rate of finished goods vs planned volume',
      'Percent of steps performed out of order',
      'Average duration of the production run',
      'Percent of orders requiring rework or reversal'
    ];

    const icons = [
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14" /><polyline points="22 4 12 14.01 9 11.01" /></svg>,
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z" /><polyline points="3.27 6.96 12 12.01 20.73 6.96" /><line x1="12" y1="22.08" x2="12" y2="12" /></svg>,
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10" /><circle cx="12" cy="12" r="6" /><circle cx="12" cy="12" r="2" /></svg>,
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z" /><line x1="12" y1="9" x2="12" y2="13" /><line x1="12" y1="17" x2="12.01" y2="17" /></svg>,
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10" /><polyline points="12 6 12 12 16 14" /></svg>,
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M21.5 2v6h-6M21.34 15.57a10 10 0 1 1-.57-8.38l5.67-5.67" /></svg>
    ];

    return (
      <div style={{ display: 'flex', flexDirection: 'row', alignItems: 'center', gap: '12px', width: '100%', height: '302px' }}>
        <KpiWheel colors={colors} label="P2I" height={302} width={140} />
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

  const ProcessTreeFlow = ({ steps }) => {
    const colors = ['#d97706', '#ca8a04', '#b45309', '#92400e', '#78350f', '#fbbf24'];
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

  const FaqModal = () => (
    <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} style={{ position: 'fixed', inset: 0, zIndex: 10000, background: 'rgba(15, 23, 42, 0.85)', backdropFilter: 'blur(12px)', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '24px' }} onClick={() => setShowFaqModal(false)}>
      <motion.div initial={{ scale: 0.9, opacity: 0, y: 20 }} animate={{ scale: 1, opacity: 1, y: 0 }} exit={{ scale: 0.9, opacity: 0, y: 20 }} style={{ background: '#fff', width: '100%', maxWidth: '800px', maxHeight: '85vh', borderRadius: '24px', position: 'relative', overflow: 'hidden', display: 'flex', flexDirection: 'column', boxShadow: '0 25px 50px -12px rgba(0, 0, 0, 0.5)' }} onClick={e => e.stopPropagation()}>
        <div style={{ padding: '32px 40px 24px', borderBottom: '1px solid #f1f5f9', display: 'flex', justifyContent: 'space-between', alignItems: 'center', background: 'linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%)' }}>
          <div>
            <h2 style={{ margin: 0, fontSize: '24px', fontWeight: 800, color: '#1e293b' }}>Frequently Asked Questions</h2>
            <p style={{ margin: '4px 0 0', fontSize: '14px', color: '#64748b' }}>Learn more about P2I Process Mining</p>
          </div>
          <button onClick={() => setShowFaqModal(false)} style={{ background: '#f1f5f9', border: 'none', width: '40px', height: '40px', borderRadius: '50%', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#64748b', fontSize: '20px', transition: 'all 0.2s' }} onMouseOver={e => e.currentTarget.style.background = '#e2e8f0'} onMouseOut={e => e.currentTarget.style.background = '#f1f5f9'}>✕</button>
        </div>
        <div style={{ padding: '24px 40px 40px', overflowY: 'auto', flex: 1 }}>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
            {P2I_FAQS.map((faq, i) => <FaqItem key={i} q={faq.q} a={faq.a} bullets={faq.bullets} />)}
          </div>
        </div>
      </motion.div>
    </motion.div>
  );

  if (introStep === 'overview') {
    return (
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', overflowY: 'auto', position: 'relative' }}>
        <AnimatePresence>{showFaqModal && <FaqModal />}</AnimatePresence>

        <div className="process-ribbon-container">
          <div className="process-ribbon-content">
            {[...shortSteps, ...shortSteps, ...shortSteps, ...shortSteps].map((s, i) => (
              <React.Fragment key={i}>
                <div className="process-ribbon-item">{s.icon}<span style={{ marginLeft: 4 }}>{s.text}</span></div>
                {i < shortSteps.length * 4 - 1 && <div className="process-ribbon-arrow">→</div>}
              </React.Fragment>
            ))}
          </div>
        </div>

        <div style={{ maxWidth: 1100, width: '100%', display: 'flex', flexDirection: 'column', gap: 16, padding: '20px 24px 30px' }}>
          <div style={{ borderBottom: '2px solid #E2E8F0', paddingBottom: 12, display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
            <div style={{ flex: 1 }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: C.amber, textTransform: 'uppercase', letterSpacing: 1, marginBottom: 4 }}>Process Overview</div>
              <h1 style={{ margin: '0 0 6px', fontSize: 22, fontWeight: 700, color: '#1e293b' }}>Plan-to-Inventory (P2I)</h1>
              <p style={{ margin: 0, fontSize: 13, color: '#475569', lineHeight: 1.6, maxWidth: 900 }}>
                Analyse your production orders, shop-floor confirmations, goods movements, and procurement chain from a single event log. Process mining helps uncover deviations, bottlenecks, missing components, and delayed production.
              </p>
            </div>
            <button onClick={() => setShowFaqModal(true)} style={{ background: `linear-gradient(135deg, ${C.amber} 0%, ${C.orange} 100%)`, color: '#fff', border: 'none', padding: '10px 20px', borderRadius: '12px', fontSize: '13px', fontWeight: 700, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '8px', boxShadow: `0 4px 12px rgba(217, 11, 6, 0.2)`, transition: 'all 0.2s', flexShrink: 0, marginTop: '10px' }} onMouseOver={e => e.currentTarget.style.transform = 'translateY(-2px)'} onMouseOut={e => e.currentTarget.style.transform = 'translateY(0)'}>
              <span>Read FAQ</span><span style={{ fontSize: '16px' }}>💬</span>
            </button>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1.3fr 1fr', gap: 24, alignItems: 'stretch' }}>
            {/* Left Column: Process Steps */}
            <div style={{ background: '#fff', border: '1px solid #E2E8F0', borderRadius: 16, padding: '16px 20px', boxShadow: '0 4px 12px rgba(0,0,0,0.04)' }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: 0.8, marginBottom: 12 }}>Process Steps</div>
              <ProcessTreeFlow steps={steps} />
            </div>

            {/* Right Column: Key Metrics */}
            <div style={{ background: '#fff', border: '1px solid #E2E8F0', borderRadius: 16, padding: '16px 20px', boxShadow: '0 4px 12px rgba(0,0,0,0.04)', display: 'flex', flexDirection: 'column' }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: 0.8, marginBottom: 12 }}>Key Metrics</div>
              <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                <KeyMetricsAnimation metrics={kpis} />
              </div>
            </div>
          </div>

          <div style={{ display: 'flex', justifyContent: 'flex-end', paddingTop: 4 }}>
            <button onClick={() => setIntroStep('choose')} style={{ background: C.amber, color: '#fff', border: 'none', padding: '10px 28px', borderRadius: 8, fontSize: 13, fontWeight: 700, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 8, boxShadow: `0 4px 12px rgba(217,119,6,0.25)`, transition: 'all 0.2s', }} onMouseOver={e => { e.currentTarget.style.background = C.amberDark; e.currentTarget.style.boxShadow = `0 6px 16px rgba(217,119,6,0.35)`; }} onMouseOut={e => { e.currentTarget.style.background = C.amber; e.currentTarget.style.boxShadow = `0 4px 12px rgba(217,119,6,0.25)`; }}>
              Continue <span style={{ fontSize: 14 }}>→</span>
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', padding: '36px 24px 48px', overflowY: 'auto' }}>
      <div style={{ maxWidth: 880, width: '100%', display: 'flex', flexDirection: 'column', gap: 24 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', width: '100%' }}>
          <button onClick={() => setIntroStep('overview')} style={{ background: 'none', border: '1px solid #E2E8F0', padding: '6px 14px', borderRadius: 6, fontSize: 12, cursor: 'pointer', color: '#64748b', fontWeight: 600 }} onMouseOver={e => e.currentTarget.style.background = '#F8FAFC'} onMouseOut={e => e.currentTarget.style.background = 'none'}>← Back to Overview</button>
          <button
            onClick={() => window.open('/snapshot p2i.pdf', '_blank')}
            style={{
              background: '#D97706', color: '#fff', border: 'none',
              padding: '7px 18px', borderRadius: 7, fontSize: 12,
              fontWeight: 700, cursor: 'pointer',
              display: 'flex', alignItems: 'center', gap: 6,
              boxShadow: '0 2px 8px rgba(217,119,6,0.25)'
            }}
            onMouseOver={e => e.currentTarget.style.background = '#b45309'}
            onMouseOut={e => e.currentTarget.style.background = '#d97706'}>
            📸 Snapshot
          </button>
        </div>
        <div style={{ textAlign: 'center' }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: 0.8, marginBottom: 8 }}>Get Started</div>
          <h2 style={{ margin: '0 0 6px', fontSize: 20, fontWeight: 700, color: '#1e293b' }}>Choose how to load your data</h2>
          <p style={{ margin: 0, fontSize: 13, color: '#64748b' }}>Select the method that matches your data format</p>
        </div>
        <div style={{ display: 'flex', width: '100%', height: 320, gap: 16 }}>
          <motion.div layout style={{ flex: hoveredSide === 'build' ? 1.7 : (hoveredSide === 'csv' ? 0.6 : 1), background: '#fff', border: '2px solid #E2E8F0', borderRadius: 16, padding: '28px 24px', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 14, textAlign: 'center', boxShadow: '0 4px 12px rgba(0,0,0,0.03)', cursor: 'pointer', overflow: 'hidden' }} onClick={onGoTableBuild} onMouseEnter={() => setHoveredSide('build')} onMouseLeave={() => setHoveredSide(null)} animate={{ borderColor: hoveredSide === 'build' ? C.amber : '#E2E8F0' }}>
            <motion.div layout style={{ width: 60, height: 60, borderRadius: 14, background: '#FFFBEB', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 28, flexShrink: 0 }}>🏭</motion.div>
            <motion.div layout style={{ flex: 1 }}>
              <motion.div layout style={{ fontSize: 18, fontWeight: 700, color: '#1e293b', marginBottom: 8, whiteSpace: hoveredSide === 'csv' ? 'nowrap' : 'normal' }}>SAP Table Upload</motion.div>
              <AnimatePresence>
                {hoveredSide !== 'csv' && (
                  <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} style={{ fontSize: 13, color: '#64748b', lineHeight: 1.65 }}>
                    Upload AFPO, AFKO (optional), AFRU, MSEG and other raw SAP tables. The transformer builds the event log automatically.
                  </motion.div>
                )}
              </AnimatePresence>
            </motion.div>
            <motion.button layout onClick={e => { e.stopPropagation(); onGoTableBuild(); }} style={{ background: C.amber, color: '#fff', border: 'none', padding: '11px 28px', borderRadius: 7, fontSize: 13, fontWeight: 700, cursor: 'pointer', width: '100%', marginTop: 'auto' }} onMouseOver={e => e.currentTarget.style.background = C.amberDark} onMouseOut={e => e.currentTarget.style.background = C.amber}>
              {hoveredSide === 'csv' ? 'Upload' : 'Upload Tables'}
            </motion.button>
          </motion.div>
          <motion.div layout style={{ flex: hoveredSide === 'csv' ? 1.7 : (hoveredSide === 'build' ? 0.6 : 1), background: '#fff', border: '2px solid #E2E8F0', borderRadius: 16, padding: '28px 24px', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 14, textAlign: 'center', boxShadow: '0 4px 12px rgba(0,0,0,0.03)', cursor: 'pointer', overflow: 'hidden' }} onClick={onGoCsvUpload} onMouseEnter={() => setHoveredSide('csv')} onMouseLeave={() => setHoveredSide(null)} animate={{ borderColor: hoveredSide === 'csv' ? '#475569' : '#E2E8F0' }}>
            <motion.div layout style={{ width: 60, height: 60, borderRadius: 14, background: '#F8FAFC', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 28, flexShrink: 0 }}>📄</motion.div>
            <motion.div layout style={{ flex: 1 }}>
              <motion.div layout style={{ fontSize: 18, fontWeight: 700, color: '#1e293b', marginBottom: 8, whiteSpace: hoveredSide === 'build' ? 'nowrap' : 'normal' }}>Pre-built CSV Upload</motion.div>
              <AnimatePresence>
                {hoveredSide !== 'build' && (
                  <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} style={{ fontSize: 13, color: '#64748b', lineHeight: 1.65 }}>
                    Upload a wide-format event log already processed by KNIME or a previous build — one row per production order.
                  </motion.div>
                )}
              </AnimatePresence>
            </motion.div>
            <motion.button layout onClick={e => { e.stopPropagation(); onGoCsvUpload(); }} style={{ background: '#475569', color: '#fff', border: 'none', padding: '11px 28px', borderRadius: 7, fontSize: 13, fontWeight: 700, cursor: 'pointer', width: '100%', marginTop: 'auto' }} onMouseOver={e => e.currentTarget.style.background = '#334155'} onMouseOut={e => e.currentTarget.style.background = '#475569'}>
              {hoveredSide === 'build' ? 'Upload' : 'Upload CSV'}
            </motion.button>
          </motion.div>
        </div>
      </div>
    </div>
  );
};

/* ─── UPLOAD BANNER (Intro + Route Selection) ─────────────────────────────────── */
const UploadBanner = React.memo(({ currentUser, onUploaded, serverOk, onLoadingChange, myFiles, fetchingFiles, handleLoadOldFile, defaultStep }) => {
  const [step, setStep] = useState(defaultStep || 'info');
  const [dragging, setDragging] = useState(false);
  const [selectedFile, setSelectedFile] = useState(null);
  const [status, setStatus] = useState('idle');
  const [msg, setMsg] = useState('');
  const [colMapping, setColMapping] = useState(null);
  const [showAvailablePopup, setShowAvailablePopup] = useState(false);
  const inputRef = useRef(null);

  useEffect(() => { if (defaultStep) setStep(defaultStep); }, [defaultStep]);

  const P2I_SCHEMA_COLS = [
    { col: 'AUFNR', desc: 'Production Order Number', req: true },
    { col: 'POSNR', desc: 'Production Order Item Number', req: true },
    { col: 'Production Order Creation', desc: 'Creation date (ERDAT from AUFK — optional)', req: false },
    { col: 'Order Confirmation', desc: 'Confirmation date (ERSDA from AFRU)', req: true },
    { col: 'Basic Start', desc: 'Planned start date (GSTRP from AFKO — optional)', req: false },
    { col: 'Scheduled Start', desc: 'Scheduled start date (GSTRS from AFKO — optional)', req: false },
    { col: 'Actual Start', desc: 'Actual start date (GSTRI from AFKO — optional)', req: false },
    { col: 'Goods Issued to Order', desc: 'GI date (BWART 261 from MSEG — optional)', req: false },
    { col: 'Finished Goods Receipt', desc: 'FG receipt date (BWART 101 from MSEG — optional)', req: false },
    { col: 'Finished Goods to Quality Inspection', desc: 'FG to QI date (BWART 131 from MSEG — optional)', req: false },
    { col: 'Operation Reversed', desc: 'Reversal of operation confirmation (optional)', req: false },
    { col: 'Goods Issued Reversed', desc: 'Reversal of GI (BWART 262 — optional)', req: false },
    { col: 'Goods transferred to subcontractor', desc: 'Transferred to subcontractor (BWART 541 — optional)', req: false },
    { col: 'Finished Goods Reversed', desc: 'Reversal of FG GR (BWART 102 — optional)', req: false },
    { col: 'Quality Inspection to Finished Goods', desc: 'Quality inspection to FG (BWART 132 — optional)', req: false },
    { col: 'PO Creation', desc: 'PO creation date', req: false },
    { col: 'GR Posting', desc: 'GR posting date', req: false },
    { col: 'Invoice Posting', desc: 'Invoice posting date', req: false },
    { col: 'BUKRS', desc: 'Company code', req: false },
    { col: 'WERKS', desc: 'Plant', req: false },
    { col: 'MATNR', desc: 'Material number', req: false },
    { col: 'ERNAM', desc: 'Order creator from AUFK', req: false },
    { col: 'Order Creation User', desc: 'AFRU operation confirmation user', req: false },
  ];

  const doUpload = async (file, mapping = {}) => {
    setStatus('uploading');
    setMsg('Uploading…');
    const form = new FormData();
    form.append('file', file);
    form.append('username', currentUser || 'Unknown');
    form.append('column_mapping', JSON.stringify(mapping));
    try {
      const r = await fetch(`${API}/p2i/upload`, { method: 'POST', body: form });
      const d = await r.json();
      if (!r.ok) throw new Error(d.detail || 'Upload failed');
      setStatus('done');
      setMsg(`Loaded ${d.unique_cases} orders (${d.input_format} format).`);
      onLoadingChange(true, 60, 'Processing event log...');
      setTimeout(() => { onUploaded(null, 'upload'); onLoadingChange(false, 100, ''); }, 800);
    } catch (e) {
      const isMappingErr = e.message?.toLowerCase().includes('missing') || e.message?.toLowerCase().includes('column');
      setStatus('error');
      setMsg(e.message);
      if (isMappingErr) onUploaded(e.message, 'upload');
    }
  };

  const handleMapCsvColumns = async () => {
    if (!selectedFile) return;
    const formPreview = new FormData();
    formPreview.append('file', selectedFile);
    try {
      const rPrev = await fetch(`${API}/p2i/transform/preview_columns`, { method: 'POST', body: formPreview });
      const dPrev = await rPrev.json();
      if (!rPrev.ok) throw new Error(dPrev.detail || 'Failed to read CSV columns');
      setColMapping({ file: selectedFile, tableDef: { name: 'Pre-built P2I CSV', required: P2I_SCHEMA_COLS.map(c => ({ col: c.col, note: c.desc })) }, uploadedCols: dPrev.columns, mapping: {} });
    } catch (e) { setStatus('error'); setMsg(e.message); }
  };

  /* ── Page 1: Info / Choose ── */
  if (step === 'info') {
    return (
      <>
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
                  background: `${C.amber}22`,
                  border: `1.5px solid ${C.amber}`,
                  borderRadius: '50%',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  margin: '0 auto 1.5rem',
                  fontSize: '28px'
                }}>
                  🔒
                </div>
                <h3 style={{ fontSize: '1.4rem', fontWeight: 800, margin: '0 0 2rem', letterSpacing: '-0.3px', color: '#fff' }}>
                  Available in Production
                </h3>
                <button
                  onClick={() => setShowAvailablePopup(false)}
                  style={{
                    background: C.amber,
                    color: '#fff',
                    border: 'none',
                    padding: '12px 30px',
                    borderRadius: '8px',
                    fontWeight: 700,
                    fontSize: '14px',
                    cursor: 'pointer',
                    width: '100%',
                    boxShadow: `0 4px 12px rgba(0,120,212,0.3)`,
                    transition: 'all 0.2s'
                  }}
                  onMouseOver={e => { e.currentTarget.style.filter = 'brightness(1.1)'; e.currentTarget.style.transform = 'translateY(-1px)'; }}
                  onMouseOut={e => { e.currentTarget.style.filter = 'none'; e.currentTarget.style.transform = 'none'; }}
                >
                  Got it
                </button>
              </motion.div>
            </motion.div>
          )}
        </AnimatePresence>

        <P2IIntroScreen
          onGoTableBuild={() => setShowAvailablePopup(true)}
          onGoCsvUpload={() => setShowAvailablePopup(true)}
          currentUser={currentUser}
        />
      </>
    );
  }

  /* ── Page 2: SAP Table Build ── */
  if (step === 'table') return (
    <TableUploadScreen onBuilt={onUploaded} onBack={() => setStep('info')} onLoadingChange={onLoadingChange} currentUser={currentUser} myFiles={myFiles} fetchingFiles={fetchingFiles} handleLoadOldFile={handleLoadOldFile} />
  );

  /* ── Page 3: Pre-built CSV ── */
  const bc = dragging ? C.amber : status === 'done' ? '#107C10' : status === 'error' ? C.red : C.border;
  const bg = dragging ? '#FFFBEB' : status === 'done' ? '#F0FAF0' : status === 'error' ? '#FDE7E9' : '#FAFAFA';
  const csvUploads = (myFiles || []).filter(f => !f.source || f.source === 'csv_upload');

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16, padding: '20px 14px' }}>
      {colMapping && (
        <div style={{ position: 'fixed', inset: 0, zIndex: 9999, background: 'rgba(0,0,0,0.5)', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 16 }} onClick={() => setColMapping(null)}>
          <div style={{ background: '#fff', borderRadius: 12, width: '100%', maxWidth: 700, maxHeight: '88vh', display: 'flex', flexDirection: 'column', boxShadow: '0 24px 64px rgba(0,0,0,0.35)' }} onClick={e => e.stopPropagation()}>
            <div style={{ padding: '20px 24px 16px', borderBottom: '1px solid #E2E8F0', flexShrink: 0 }}>
              <div style={{ fontSize: 10, fontWeight: 700, color: C.amber, textTransform: 'uppercase', letterSpacing: 1, marginBottom: 4 }}>Column Mapping</div>
              <div style={{ fontSize: 17, fontWeight: 700, color: '#1e293b' }}>Map Columns for {colMapping.tableDef.name}</div>
            </div>
            <div style={{ overflowY: 'auto', flex: 1 }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                <thead style={{ position: 'sticky', top: 0, zIndex: 2 }}>
                  <tr style={{ background: '#F8FAFC' }}><th style={{ padding: '10px 16px', textAlign: 'left', fontWeight: 700, color: '#64748b', fontSize: 10, borderBottom: '2px solid #E2E8F0', width: '35%' }}>Column</th><th style={{ padding: '10px 12px', textAlign: 'left', fontWeight: 700, color: '#64748b', fontSize: 10, borderBottom: '2px solid #E2E8F0', width: '35%' }}>Map to File Column</th><th style={{ padding: '10px 12px', textAlign: 'left', fontWeight: 700, color: '#64748b', fontSize: 10, borderBottom: '2px solid #E2E8F0' }}>Purpose</th></tr>
                </thead>
                <tbody>
                  {colMapping.tableDef.required.map((r) => {
                    const autoMatch = colMapping.uploadedCols.find(c => c.toUpperCase() === r.col.toUpperCase());
                    const selected = colMapping.mapping[r.col] !== undefined ? colMapping.mapping[r.col] : (autoMatch || '');
                    return (<tr key={r.col} style={{ borderBottom: '1px solid #F1F5F9' }}><td style={{ padding: '10px 16px', fontFamily: 'monospace', fontWeight: 700, color: '#334155', fontSize: 13 }}>{r.col}</td><td style={{ padding: '10px 12px' }}><select value={selected} onChange={e => setColMapping(p => ({ ...p, mapping: { ...p.mapping, [r.col]: e.target.value } }))} style={{ width: '100%', padding: '6px 8px', borderRadius: 4, border: '1px solid #CBD5E1', background: '#fff', fontSize: 12 }}><option value="">-- Leave Blank --</option>{colMapping.uploadedCols.map(c => (<option key={c} value={c}>{c}</option>))}</select></td><td style={{ padding: '10px 12px', color: '#64748b', fontSize: 11 }}>{r.note}</td></tr>);
                  })}
                </tbody>
              </table>
            </div>
            <div style={{ padding: '14px 20px', borderTop: '1px solid #E2E8F0', flexShrink: 0, display: 'flex', justifyContent: 'flex-end', gap: 12 }}>
              <button onClick={() => setColMapping(null)} style={{ padding: '8px 16px', background: '#fff', color: '#64748b', border: '1px solid #CBD5E1', borderRadius: 6, fontSize: 13, fontWeight: 700, cursor: 'pointer' }}>Cancel</button>
              <button onClick={() => {
                const finalMapping = {};
                colMapping.tableDef.required.forEach(r => {
                  const autoMatch = colMapping.uploadedCols.find(c => c.toUpperCase() === r.col.toUpperCase());
                  const sel = colMapping.mapping[r.col] !== undefined ? colMapping.mapping[r.col] : (autoMatch || '');
                  if (sel && sel !== r.col) finalMapping[sel] = r.col;
                });
                setColMapping(null);
                doUpload(colMapping.file, finalMapping);
              }} style={{ padding: '8px 16px', background: C.amber, color: '#fff', border: 'none', borderRadius: 6, fontSize: 13, fontWeight: 700, cursor: 'pointer' }}>Confirm & Upload</button>
            </div>
          </div>
        </div>
      )}
      <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
        <button onClick={() => setStep('info')} style={{ background: 'none', border: '1px solid #E2E8F0', padding: '5px 12px', borderRadius: 6, fontSize: 12, cursor: 'pointer', color: '#64748b', fontWeight: 600 }} onMouseOver={e => e.currentTarget.style.background = '#F8FAFC'} onMouseOut={e => e.currentTarget.style.background = 'none'}>← Back</button>
        <div style={{ fontSize: 11, fontWeight: 700, color: C.amber, textTransform: 'uppercase', letterSpacing: 0.8 }}>Upload Pre-built P2I CSV</div>
      </div>
      <div onDragOver={e => { e.preventDefault(); setDragging(true); }} onDragLeave={() => setDragging(false)} onDrop={e => { e.preventDefault(); setDragging(false); const f = e.dataTransfer.files[0]; if (f) { setSelectedFile(f); setStatus('idle'); setMsg(''); } }} onClick={() => { if (status !== 'uploading' && inputRef.current) { inputRef.current.value = ''; inputRef.current.click(); } }} style={{ border: `2px dashed ${bc}`, borderRadius: 8, padding: '14px 24px', background: bg, cursor: 'pointer', textAlign: 'center', transition: 'all .2s', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 14, flexDirection: selectedFile ? 'column' : 'row' }}>
        <input ref={inputRef} type="file" accept=".csv" style={{ display: 'none' }} onChange={e => { const f = e.target.files[0]; if (f) { setSelectedFile(f); setStatus('idle'); setMsg(''); } }} />
        <div style={{ display: 'flex', alignItems: 'center', gap: 14 }}>
          <div style={{ fontSize: 22, fontWeight: 'bold', color: status === 'done' ? '#107C10' : status === 'error' ? '#D13438' : C.amber }}>{status === 'done' ? '✓' : status === 'error' ? '✕' : '⬆'}</div>
          <div style={{ textAlign: 'left' }}>
            <div style={{ fontSize: 13, fontWeight: 700, color: '#323130' }}>{selectedFile ? selectedFile.name : 'Click or drag & drop a CSV file here'}</div>
            <div style={{ fontSize: 11, color: C.slate, marginTop: 2 }}>{msg || 'Wide-format or KNIME long-format CSV — one row per AUFNR'}</div>
          </div>
        </div>
        {selectedFile && status !== 'uploading' && (
          <div style={{ display: 'flex', gap: 12, marginTop: 4 }}>
            <button onClick={e => { e.stopPropagation(); handleMapCsvColumns(); }} style={{ fontSize: 12, padding: '8px 16px', background: '#FFFBEB', color: C.amberDark, border: `1px solid ${C.amber}`, borderRadius: 6, cursor: 'pointer', fontWeight: 700 }}>Map Columns</button>
            <button onClick={e => { e.stopPropagation(); doUpload(selectedFile, {}); }} style={{ fontSize: 12, padding: '8px 16px', background: C.amber, color: '#fff', border: 'none', borderRadius: 6, cursor: 'pointer', fontWeight: 700 }}>Upload</button>
            <button onClick={e => { e.stopPropagation(); setStatus('idle'); setMsg(''); setSelectedFile(null); }} style={{ fontSize: 12, padding: '8px 16px', background: '#fff', border: `1px solid ${C.border}`, borderRadius: 6, cursor: 'pointer', color: C.slate }}>Clear</button>
          </div>
        )}
      </div>
      {csvUploads.length > 0 && (
        <div style={{ background: '#fff', border: `1px solid ${C.border}`, borderRadius: 10, padding: '18px 20px' }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: 0.8, marginBottom: 12 }}>Previous CSV Uploads</div>
          <div style={{ border: '1px solid #E2E8F0', borderRadius: 8, overflow: 'hidden' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
              <thead style={{ background: '#F3F2F1', textAlign: 'left' }}>
                <tr><th style={{ padding: '9px 14px', fontWeight: 600 }}>File Name</th><th style={{ padding: '9px 14px', fontWeight: 600 }}>Date</th><th style={{ padding: '9px 14px', fontWeight: 600, textAlign: 'right' }}>Orders</th><th style={{ padding: '9px 14px', fontWeight: 600 }}>Action</th></tr>
              </thead>
              <tbody>
                {csvUploads.map((f, idx) => (<tr key={idx} style={{ borderBottom: '1px solid #E2E8F0' }} onMouseEnter={e => e.currentTarget.style.background = '#F8FAFC'} onMouseLeave={e => e.currentTarget.style.background = 'transparent'}><td style={{ padding: '9px 14px', fontWeight: 600, color: '#1e293b' }}>{f.filename}</td><td style={{ padding: '9px 14px', color: '#64748b', fontSize: 11 }}>{f.upload_date}</td><td style={{ padding: '9px 14px', fontWeight: 600, textAlign: 'right' }}>{f.cases != null ? Number(f.cases).toLocaleString() : '—'}</td><td style={{ padding: '9px 14px' }}><button onClick={() => handleLoadOldFile(f.file_id)} style={{ background: C.amber, color: '#fff', border: 'none', padding: '5px 12px', borderRadius: 4, cursor: 'pointer', fontSize: 11, fontWeight: 600 }}>Load Dashboard</button></td></tr>))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
});

/* ════════════════════════════════════════════
   P2P PROCESS MAP MODAL
════════════════════════════════════════════ */
const P2P_HAPPY = [
  /* ── Happy Path (middle lane, y = 260) ── */
  { id: 'p2p-pr-create', label: 'PR Creation', x: 50, y: 260, isHappy: true },
  { id: 'p2p-pr-release', label: 'PR Release Date', x: 50 + NODE_W, y: 260, isHappy: true },
  { id: 'p2p-po-create', label: 'PO Creation', x: 50 + NODE_W * 2, y: 260, isHappy: true },
  { id: 'p2p-po-date', label: 'PO Date', x: 50 + NODE_W * 3, y: 260, isHappy: true },
  { id: 'p2p-gr', label: 'GR Posting', x: 50 + NODE_W * 4, y: 260, isHappy: true },
  { id: 'p2p-inv', label: 'Invoice Posting', x: 50 + NODE_W * 5, y: 260, isHappy: true },
  /* ── ABOVE lane — primary reversals (y = 60), mirrors P2P SIDE_ABOVE_LR ── */
  { id: 'p2p-pr-rev', label: 'PR Reversal', x: 50 + NODE_W, y: 60, isHappy: false },
  { id: 'p2p-pr-rev2', label: 'PR Reversal (Post-PO)', x: 50 + NODE_W * 2, y: 60, isHappy: false },
  { id: 'p2p-po-rev', label: 'PO Reversal', x: 50 + NODE_W * 3, y: 60, isHappy: false },
  { id: 'p2p-gr-rev', label: 'GR Reversal', x: 50 + NODE_W * 4, y: 60, isHappy: false },
  { id: 'p2p-inv-rev', label: 'Invoice Reversal Date', x: 50 + NODE_W * 5, y: 60, isHappy: false },
  /* ── BELOW lane — post-step reversals (y = 460), mirrors P2P SIDE_BELOW_LR ── */
  { id: 'p2p-po-rev2', label: 'PO Reversal (Post-GR)', x: 50 + NODE_W * 3, y: 460, isHappy: false },
  { id: 'p2p-gr-rev2', label: 'GR Reversal (Post-Inv)', x: 50 + NODE_W * 4, y: 460, isHappy: false },
];

const P2P_EDGES_DEF = [
  /* Happy path chain */
  { s: 'p2p-pr-create', t: 'p2p-pr-release', sh: 'right-s', th: 'left-t' },
  { s: 'p2p-pr-release', t: 'p2p-po-create', sh: 'right-s', th: 'left-t' },
  { s: 'p2p-po-create', t: 'p2p-po-date', sh: 'right-s', th: 'left-t' },
  { s: 'p2p-po-date', t: 'p2p-gr', sh: 'right-s', th: 'left-t' },
  { s: 'p2p-gr', t: 'p2p-inv', sh: 'right-s', th: 'left-t' },
  /* Happy → ABOVE deviations */
  { s: 'p2p-pr-release', t: 'p2p-pr-rev', sh: 'top-s', th: 'bottom-t', dev: true },
  { s: 'p2p-po-create', t: 'p2p-pr-rev2', sh: 'top-s', th: 'bottom-t', dev: true },
  { s: 'p2p-po-date', t: 'p2p-po-rev', sh: 'top-s', th: 'bottom-t', dev: true },
  { s: 'p2p-gr', t: 'p2p-gr-rev', sh: 'top-s', th: 'bottom-t', dev: true },
  { s: 'p2p-inv', t: 'p2p-inv-rev', sh: 'top-s', th: 'bottom-t', dev: true },
  /* Happy → BELOW deviations */
  { s: 'p2p-po-date', t: 'p2p-po-rev2', sh: 'bottom-s', th: 'top-t', dev: true },
  { s: 'p2p-gr', t: 'p2p-gr-rev2', sh: 'bottom-s', th: 'top-t', dev: true },
];

const P2pProcessModal = React.memo(({ onClose }) => {
  const rfNodesP2P = P2P_HAPPY.map(n => ({
    id: n.id, type: 'processNode', position: { x: n.x, y: n.y },
    data: {
      label: n.label, is_main: n.isHappy, frequency: 0, maxFreq: 1,
      _overrideBg: n.isHappy ? '#fffbeb' : '#FEF2F2',
      _overrideBorder: n.isHappy ? '#d97706' : '#F87171',
      _overrideText: n.isHappy ? '#78350F' : '#991B1B',
      _overrideBadge: n.isHappy ? '#d97706' : '#DC2626',
    },
  }));
  const rfEdgesP2P = P2P_EDGES_DEF.map((e, i) => {
    const col = e.dev ? '#F87171' : '#d97706';
    return {
      id: `p2p-e-${i}`, source: e.s, target: e.t,
      sourceHandle: e.sh, targetHandle: e.th,
      type: 'freqEdge',
      markerEnd: { type: MarkerType.ArrowClosed, color: col, width: 14, height: 14 },
      style: e.dev ? { stroke: col, strokeWidth: 1.5, strokeDasharray: '5 4' } : {},
      data: { frequency: 0, avg_days: null, maxFreq: 1, curvature: 0.5, edgeColor: col },
    };
  });

  return (
    <div style={{
      position: 'fixed', inset: 0, zIndex: 9999, background: 'rgba(0,0,0,0.65)',
      display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 24
    }}
      onClick={onClose}>
      <div style={{
        background: '#F0F2F5', borderRadius: 12, width: '92vw', maxWidth: 1400,
        height: '82vh', display: 'flex', flexDirection: 'column',
        boxShadow: '0 24px 64px rgba(0,0,0,0.4)', overflow: 'hidden'
      }}
        onClick={e => e.stopPropagation()}>

        {/* Modal header */}
        <div style={{
          background: '#78350F', padding: '14px 20px', display: 'flex',
          justifyContent: 'space-between', alignItems: 'center', flexShrink: 0
        }}>
          <div>
            <div style={{ fontSize: 17, fontWeight: 800, color: '#fff', letterSpacing: .3 }}>
              🔗 Procure-to-Pay (P2P) Process Map
            </div>
            <div style={{ fontSize: 11, color: 'rgba(255,255,255,.55)', marginTop: 2 }}>
              Standard P2P flow — golden = happy path &nbsp;|&nbsp; red = deviation / reversal
            </div>
          </div>
          {/* Legend */}
          <div style={{ display: 'flex', gap: 16, alignItems: 'center', marginRight: 16 }}>
            {[['#d97706', 'Happy Path'], ['#F87171', 'Deviation / Reversal']].map(([c, l]) => (
              <div key={l} style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <div style={{ width: 12, height: 12, borderRadius: 2, background: c }} />
                <span style={{ fontSize: 11, color: 'rgba(255,255,255,.75)' }}>{l}</span>
              </div>
            ))}
          </div>
          <button onClick={onClose}
            style={{
              background: 'rgba(255,255,255,.12)', border: 'none', color: '#fff',
              width: 32, height: 32, borderRadius: 6, fontSize: 18, cursor: 'pointer',
              display: 'flex', alignItems: 'center', justifyContent: 'center'
            }}>✕</button>
        </div>

        {/* Flow canvas */}
        <div style={{ flex: 1, minHeight: 0 }}>
          <ReactFlow nodes={rfNodesP2P} edges={rfEdgesP2P}
            nodeTypes={nodeTypes} edgeTypes={edgeTypes}
            fitView fitViewOptions={{ padding: 0.2 }} minZoom={0.05} maxZoom={2}
            panOnScroll zoomOnScroll>
            <Background color="#E1DFDD" gap={24} />
            <Controls />
            <MiniMap nodeColor={n => n.data?.is_main ? '#d97706' : '#F87171'}
              style={{ background: '#F8FAFC', border: '1px solid #E1DFDD', borderRadius: 6 }} />
          </ReactFlow>
        </div>
      </div>
    </div>
  );
});

/* ════════════════════════════════════════════
   MAIN P2I DASHBOARD
════════════════════════════════════════════ */
export default function P2IDashboard({ currentUser, onSignOut, onBackHome }) {
  const [serverOk, setServerOk] = useState(false);
  const [dataLoaded, setDataLoaded] = useState(false);
  const [mappingError, setMappingError] = useState(null);
  const [loading, setLoading] = useState(false);
  const [dashboardLoading, setDashboardLoading] = useState(false);
  const [chartsReady, setChartsReady] = useState(false);
  const [pmReady, setPmReady] = useState(false);
  const intentToUpload = useRef(true);
  const [loadProg, setLoadProg] = useState(0);
  const [loadLabel, setLoadLabel] = useState('');
  const [filters, setFilters] = useState({});
  const [activeTab, setActiveTab] = useState('process');
  const [tabSkeleton, setTabSkeleton] = useState(false);
  const [layoutDir, setLayoutDir] = useState('TB');
  const [uploadStepOverride, setUploadStepOverride] = useState(null);

  const [selected, setSelected] = useState({
    company: 'ALL', plant: 'ALL', matnr: 'ALL', auart: 'ALL', kostl: 'ALL', fevor: 'ALL',
    ekgrp: 'ALL', lifnr: 'ALL', vendor: 'ALL', case_id: 'ALL',
    month: 'ALL', year: 'ALL', quarter: 'ALL', lead_time: 'ALL', ernam: 'ALL', status: 'ALL'
  });
  const [crossFilter, setCrossFilter] = useState(null);

  const [kpis, setKpis] = useState(null);
  const [actData, setActData] = useState([]);
  const [monData, setMonData] = useState([]);
  const [compData, setCompData] = useState([]);
  const [plantData, setPlantData] = useState([]);
  const [matnrData, setMatnrData] = useState([]);
  const [vendData, setVendData] = useState([]);
  const [ltData, setLtData] = useState([]);
  const [ernamData, setErnamData] = useState([]);
  const [caseTableData, setCaseTableData] = useState([]);
  const [caseEvents, setCaseEvents] = useState([]);
  const [happyPathData, setHappyPathData] = useState([]);
  const [bottleneckData, setBottleneckData] = useState([]);
  const [opReversalData, setOpReversalData] = useState([]);
  const [giConfirmData, setGiConfirmData] = useState([]);
  const [plantLtData, setPlantLtData] = useState([]);
  const [poRevTimeline, setPoRevTimeline] = useState([]);

  const [pmLoading, setPmLoading] = useState(false);
  const [pmError, setPmError] = useState('');
  const [showP2pModal, setShowP2pModal] = useState(false);
  const [rfNodes, setRfNodes, onNodesChange] = useNodesState([]);
  const [rfEdges, setRfEdges, onEdgesChange] = useEdgesState([]);
  const [rawGraphData, setRawGraphData] = useState(null);
  const [refreshTrigger, setRefreshTrigger] = useState(0);
  const [myFiles, setMyFiles] = useState([]);
  const [fetchingFiles, setFetchingFiles] = useState(false);

  useEffect(() => {
    if (currentUser && !dataLoaded) {
      setFetchingFiles(true);
      fetch(`${API}/p2i/my_files?username=${currentUser}`).then(r => r.ok ? r.json() : []).then(d => setMyFiles(d)).catch(() => { }).finally(() => setFetchingFiles(false));
    }
  }, [currentUser, dataLoaded, refreshTrigger]);

  const handleLoadOldFile = async (file_id) => {
    handleLoadingChange(true, 50, 'Loading previous dashboard...');
    try {
      const r = await fetch(`${API}/p2i/load_file`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username: currentUser, file_id }) });
      if (!r.ok) throw new Error('Failed to load file');
      intentToUpload.current = false;
      setDataLoaded(true);
      handleRefresh();
    } catch (e) { alert('Error loading dashboard: ' + e.message); }
    finally { setTimeout(() => handleLoadingChange(false, 100, ''), 500); }
  };

  const logAction = useCallback((action, details) => {
    if (!currentUser) return;
    fetch(`${API}/p2i/log`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username: currentUser, action, details }) }).catch(() => { });
  }, [currentUser]);

  const handleLoadingChange = useCallback((vis, prog, lbl) => { setLoading(vis); setLoadProg(prog); setLoadLabel(lbl); }, []);

  const onUploaded = (err, source) => {
    if (source) setUploadStepOverride(source);
    setMappingError(err || null);
    intentToUpload.current = false;
    setDataLoaded(true);
    handleRefresh();
  };

  const handleSignOut = async () => {
    logAction('LOGOUT', 'User signed out');
    try { await fetch(`${API}/p2i/clear`, { method: 'POST' }); } catch (e) { }
    intentToUpload.current = true;
    setDataLoaded(false); setChartsReady(false); setPmReady(false); setKpis(null);
    if (onSignOut) onSignOut();
  };

  const handleFixMapping = (sourceType) => {
    logAction('FIX_MAPPING', 'Navigated to column mapping');
    intentToUpload.current = true;
    setDataLoaded(false); setChartsReady(false); setPmReady(false);
    setUploadStepOverride(sourceType || 'table');
  };

  const handleResetData = () => {
    logAction('RESET_DATA', 'Started upload new file flow');
    setMappingError(null);
    intentToUpload.current = true;
    setDataLoaded(false); setChartsReady(false); setPmReady(false);
    setUploadStepOverride(null); setKpis(null); setActData([]);
    setCaseTableData([]); setCaseEvents([]); setErnamData([]);
    setSelected({ company: 'ALL', plant: 'ALL', matnr: 'ALL', auart: 'ALL', kostl: 'ALL', fevor: 'ALL', ekgrp: 'ALL', lifnr: 'ALL', vendor: 'ALL', case_id: 'ALL', month: 'ALL', year: 'ALL', quarter: 'ALL', lead_time: 'ALL', ernam: 'ALL', status: 'ALL' });
    setCrossFilter(null);
  };

  const handleRefresh = () => { logAction('REFRESH', 'Refreshed the dashboard'); setRefreshTrigger(p => p + 1); };

  const onNodeDoubleClick = useCallback((_, node) => {
    if (node.id === P2P_GATEWAY_ID) {
      logAction('P2P_OPEN', 'Opened P2P Process Map');
      setShowP2pModal(true);
    }
  }, [logAction]);

  useEffect(() => {
    if (!currentUser) return;
    const ping = () => fetch(`${API}/`).then(r => r.ok ? r.json() : null).then(d => { setServerOk(!!(d?.status)); }).catch(() => setServerOk(false));
    ping(); const t = setInterval(ping, 5000); return () => clearInterval(t);
  }, [currentUser]);

  const baseQStr = useCallback(() => {
    const q = qs(selected);
    const u = `username=${encodeURIComponent(currentUser || 'Unknown')}`;
    return q ? `${q}&${u}` : `?${u}`;
  }, [selected, currentUser]);

  const effectiveQStr = useCallback(() => {
    let q;
    if (!crossFilter || !CROSS_TO_PARAM[crossFilter.type]) q = qs(selected);
    else q = qs({ ...selected, [CROSS_TO_PARAM[crossFilter.type]]: crossFilter.value });
    const u = `username=${encodeURIComponent(currentUser || 'Unknown')}`;
    return q ? `${q}&${u}` : `?${u}`;
  }, [crossFilter, selected, currentUser]);

  const handleSelect = useCallback((type, value) => {
    setCrossFilter(prev => {
      const isRemoving = prev?.type === type && prev?.value === value;
      if (isRemoving) logAction('FILTER', `Cleared cross-filter: ${type}`);
      else logAction('FILTER', `Applied cross-filter: ${type} = ${value}`);
      return isRemoving ? null : { type, value };
    });
  }, [logAction]);

  const clearCF = useCallback(() => { logAction('FILTER', 'Cleared cross-filters'); setCrossFilter(null); }, [logAction]);

  useEffect(() => {
    if (!dataLoaded) return;
    fetch(`${API}/p2i/filters${baseQStr()}`).then(r => r.ok ? r.json() : {}).then(d => setFilters(d && typeof d === 'object' && !Array.isArray(d) ? d : {})).catch(() => setFilters({}));
  }, [baseQStr, dataLoaded, refreshTrigger]);

  useEffect(() => {
    if (!dataLoaded) return;
    if (selected.case_id !== 'ALL' && selected.case_id != null) {
      fetch(`${API}/p2i/case_events?case_id=${encodeURIComponent(selected.case_id)}&username=${encodeURIComponent(currentUser || 'Unknown')}`).then(r => r.ok ? r.json() : []).then(setCaseEvents).catch(() => setCaseEvents([]));
    } else setCaseEvents([]);
  }, [selected.case_id, dataLoaded, refreshTrigger, currentUser]);

  useEffect(() => {
    if (!dataLoaded) return;
    if (chartsReady) setDashboardLoading(true);
    const cq = effectiveQStr();
    const arr = (u, s) => fetch(u).then(r => r.ok ? r.json() : []).then(d => s(Array.isArray(d) ? d : [])).catch(() => s([]));
    const promises = [
      fetch(`${API}/p2i/kpis${cq}`).then(r => { if (!r.ok) return {}; return r.json(); }).then(d => setKpis(d)).catch(() => setKpis({})),
      arr(`${API}/p2i/charts/activity${cq}`, setActData),
      arr(`${API}/p2i/charts/monthly${cq}`, setMonData),
      arr(`${API}/p2i/charts/company${cq}`, setCompData),
      arr(`${API}/p2i/charts/plant${cq}`, setPlantData),
      arr(`${API}/p2i/charts/material${cq}`, setMatnrData),
      arr(`${API}/p2i/charts/vendors${cq}`, setVendData),
      arr(`${API}/p2i/charts/leadtime${cq}`, setLtData),
      arr(`${API}/p2i/charts/ernam${cq}`, setErnamData),
      arr(`${API}/p2i/cases${cq}`, setCaseTableData),
      arr(`${API}/p2i/charts/happy_path${cq}`, setHappyPathData),
      arr(`${API}/p2i/charts/bottleneck${cq}`, setBottleneckData),
      arr(`${API}/p2i/charts/operation_reversals${cq}`, setOpReversalData),
      arr(`${API}/p2i/charts/gi_before_confirm_ernam${cq}`, setGiConfirmData),
      arr(`${API}/p2i/charts/plant_lead_time${cq}`, setPlantLtData),
      arr(`${API}/p2i/charts/po_rev_timeline${cq}`, setPoRevTimeline),
    ];
    Promise.all(promises).finally(() => { setDashboardLoading(false); setChartsReady(true); });
  }, [effectiveQStr, dataLoaded, refreshTrigger]);

  useEffect(() => {
    if (!dataLoaded) return;
    const qStr = effectiveQStr();
    if (pmReady) setPmLoading(true);
    setPmError('');
    fetch(`${API}/p2i/process-map${qStr}`).then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); }).then(d => { setRawGraphData(d); buildFlowMap(d.nodes, d.edges, setRfNodes, setRfEdges, layoutDir); }).catch(err => setPmError(`Failed: ${err.message}`)).finally(() => { setPmLoading(false); setPmReady(true); });
  }, [effectiveQStr, dataLoaded, refreshTrigger]);

  useEffect(() => { if (dataLoaded && chartsReady && pmReady && loading) setLoading(false); }, [dataLoaded, chartsReady, pmReady, loading]);
  useEffect(() => { if (rawGraphData) buildFlowMap(rawGraphData.nodes, rawGraphData.edges, setRfNodes, setRfEdges, layoutDir); }, [layoutDir, rawGraphData]);

  const slicer = (key, label, filterKey) => {
    const raw = filters[filterKey];
    const opts = Array.isArray(raw) ? raw : ['ALL'];
    const deduped = opts[0] === 'ALL' ? opts : ['ALL', ...opts];
    const handleChange = (val) => { logAction('FILTER', `Changed slicer ${key} to ${val}`); setSelected(prev => ({ ...prev, [key]: val })); setCrossFilter(null); };
    if (['case_id', 'vendor', 'lifnr', 'matnr'].includes(key)) return <SearchableSelect key={key} label={label} value={selected[key] || 'ALL'} options={deduped} onChange={handleChange} />;
    return <FilterSelect key={key} label={label} value={selected[key] || 'ALL'} options={deduped} onChange={handleChange} />;
  };

  const resetAll = () => {
    logAction('FILTER', 'Reset all slicers');
    setSelected({ company: 'ALL', plant: 'ALL', matnr: 'ALL', auart: 'ALL', kostl: 'ALL', fevor: 'ALL', ekgrp: 'ALL', lifnr: 'ALL', vendor: 'ALL', case_id: 'ALL', month: 'ALL', year: 'ALL', quarter: 'ALL', lead_time: 'ALL', ernam: 'ALL', status: 'ALL' });
    setCrossFilter(null);
  };

  const hasActiveFilters = Object.values(selected).some(v => v && v !== 'ALL') || !!crossFilter;

  return (
    <div style={{ fontFamily: "'Segoe UI',-apple-system,sans-serif", background: C.bg, height: '100vh', display: 'flex', flexDirection: 'column' }}>
      <style>{`
        @keyframes spin{to{transform:rotate(360deg)}}
        @keyframes cometFlow{from{stroke-dashoffset:200}to{stroke-dashoffset:0}}
        *{box-sizing:border-box}
        ::-webkit-scrollbar{width:5px;height:5px}
        ::-webkit-scrollbar-track{background:transparent}
        ::-webkit-scrollbar-thumb{background:#D2D0CE;border-radius:3px}
        ::-webkit-scrollbar-thumb:hover{background:#A19F9D}
        .tab-button{padding:8px 16px;font-size:11px;font-weight:600;background:transparent;color:rgba(255,255,255,0.75);border:none;cursor:pointer;transition:all 0.2s;white-space:nowrap}
        .tab-button:hover{background:rgba(255,255,255,0.1);color:#fff}
        .tab-button.active{background:rgba(217,119,6,0.2);color:#D97706;border-bottom:2px solid #D97706}
        .skeleton-shimmer{background:linear-gradient(90deg,#F3F2F1 25%,#E8E6E3 50%,#F3F2F1 75%);background-size:200% 100%;animation:shimmer 1.5s infinite}
        @keyframes shimmer{0%{background-position:200% 0}100%{background-position:-200% 0}}
        .tab-skeleton-card{background:#fff;border-radius:8px;padding:14px;border:1px solid #E1DFDD;display:flex;flex-direction:column;gap:10;animation:shimmer 1.8s infinite}
        .tab-skeleton-title{height:16px;border-radius:4px;background:linear-gradient(90deg,#F3F2F1 25%,#E8E6E3 50%,#F3F2F1 75%);background-size:200% 100%;animation:shimmer 1.5s infinite;width:55%}
        .tab-skeleton-sub{height:12px;border-radius:3px;background:linear-gradient(90deg,#F3F2F1 25%,#E8E6E3 50%,#F3F2F1 75%);background-size:200% 100%;animation:shimmer 1.5s infinite;width:35%}
        .tab-skeleton-chart{border-radius:6px;background:linear-gradient(90deg,#F3F2F1 25%,#E8E6E3 50%,#F3F2F1 75%);background-size:200% 100%;animation:shimmer 1.5s infinite}
.tab-skeleton-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:10px}
        @media print { .no-print { display: none !important; } }
      `}</style>

      <LoadingOverlay visible={loading} progress={loadProg} label={loadLabel} />

      {/* ─── P2P Process Map Modal ─── */}
      {showP2pModal && <P2pProcessModal onClose={() => setShowP2pModal(false)} />}

      {/* ─── Header ─── */}
      <div style={{ background: C.headerBg, padding: '10px 20px', flexShrink: 0, display: 'flex', justifyContent: 'space-between', alignItems: 'center', boxShadow: '0 2px 8px rgba(0,0,0,.2)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <img src="/logo.png" alt="AJALabs Logo" onClick={() => onBackHome && onBackHome()} title="Back to Home" style={{ height: '36px', objectFit: 'contain', cursor: 'pointer', borderRadius: 4 }} onMouseOver={e => e.currentTarget.style.opacity = '0.7'} onMouseOut={e => e.currentTarget.style.opacity = '1'} />
          <div>
            <div style={{ fontWeight: 700, fontSize: 16, color: '#fff' }}>P2I Process Explorer</div>
            <div style={{ fontSize: 10, color: 'rgba(255,255,255,.5)' }}>Plan-to-Inventory Process Mining</div>
          </div>
          {crossFilter && (<div style={{ display: 'flex', alignItems: 'center', gap: 6, marginLeft: 16, background: 'rgba(217,119,6,0.2)', border: '1px solid rgba(217,119,6,0.4)', borderRadius: 6, padding: '4px 12px', fontSize: 12 }}><span style={{ color: '#D97706', fontWeight: 600 }}>Filter: {crossFilter.type}: <strong>{crossFilter.value}</strong></span><button onClick={clearCF} style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'rgba(255,255,255,.8)', fontWeight: 700, fontSize: 14, padding: '0 2px' }}>X</button></div>)}
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          {dataLoaded && kpis && (<div style={{ fontSize: 11, color: 'rgba(255,255,255,.5)' }}>{Number(kpis.total_orders || 0).toLocaleString()} order items loaded</div>)}
          {dataLoaded && (
            <div style={{ display: 'flex', alignItems: 'stretch', gap: 0, background: 'rgba(255,255,255,0.08)', borderRadius: 6, border: '1px solid rgba(255,255,255,0.15)', overflow: 'hidden' }}>
              <button className={`tab-button ${activeTab === 'process' ? 'active' : ''}`} onClick={() => { if (activeTab !== 'process') { logAction('TAB', 'Viewed Process Mining'); setActiveTab('process'); setTabSkeleton(true); setTimeout(() => setTabSkeleton(false), 500); } }}>Process Mining</button>
              <button className={`tab-button ${activeTab === 'dimensions' ? 'active' : ''}`} onClick={() => { if (activeTab !== 'dimensions') { logAction('TAB', 'Viewed EDA'); setActiveTab('dimensions'); setTabSkeleton(true); setTimeout(() => setTabSkeleton(false), 500); } }}>EDA</button>
              <button onClick={() => { window.open(`${API}/p2i/download_output?username=${encodeURIComponent(currentUser || 'Unknown')}`, '_blank'); logAction('DOWNLOAD', 'Downloaded P2I output CSV'); }} style={{ fontSize: 11, fontWeight: 600, background: 'transparent', color: 'rgba(255,255,255,0.75)', border: 'none', borderLeft: '1px solid rgba(255,255,255,0.15)', padding: '8px 14px', cursor: 'pointer', whiteSpace: 'nowrap' }} onMouseOver={e => { e.currentTarget.style.background = 'rgba(217,119,6,0.15)'; e.currentTarget.style.color = '#D97706'; }} onMouseOut={e => { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.color = 'rgba(255,255,255,0.75)'; }}>⬇ Export CSV</button>
              <button onClick={handleResetData} style={{ fontSize: 11, fontWeight: 600, background: 'transparent', color: 'rgba(255,255,255,0.75)', border: 'none', borderLeft: '1px solid rgba(255,255,255,0.15)', padding: '8px 14px', cursor: 'pointer', whiteSpace: 'nowrap' }} onMouseOver={e => { e.currentTarget.style.background = 'rgba(255,255,255,0.12)'; e.currentTarget.style.color = '#fff'; }} onMouseOut={e => { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.color = 'rgba(255,255,255,0.75)'; }}>📂 Upload New File</button>
            </div>
          )}
          <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginLeft: 16 }}>
            <div style={{ width: 1, height: 24, background: 'rgba(255,255,255,0.2)' }} />
            <div style={{ fontSize: 12, color: 'rgba(255,255,255,0.7)' }}>User: <strong style={{ color: '#fff' }}>{currentUser}</strong></div>
            <button onClick={handleSignOut} style={{ background: 'rgba(209,52,56,0.85)', color: '#fff', border: 'none', padding: '6px 12px', borderRadius: 4, cursor: 'pointer', fontSize: 11, fontWeight: 700 }} onMouseOver={e => e.currentTarget.style.background = '#D13438'} onMouseOut={e => e.currentTarget.style.background = 'rgba(209,52,56,0.85)'}>Sign Out</button>
          </div>
        </div>
      </div>

      {/* ─── Main Content ─── */}
      <div style={{ flex: 1, overflowY: 'auto', padding: '12px 14px 40px', display: 'flex', flexDirection: 'column', gap: 10 }}>

        {!dataLoaded && (
          <UploadBanner currentUser={currentUser} onUploaded={onUploaded} serverOk={serverOk} onLoadingChange={handleLoadingChange} myFiles={myFiles} fetchingFiles={fetchingFiles} handleLoadOldFile={handleLoadOldFile} defaultStep={uploadStepOverride} />
        )}

        {dataLoaded && (mappingError || (kpis && kpis.total_orders === 0)) && (
          <div style={{ background: '#FFF4CE', border: '1px solid #FDE7E9', borderRadius: 8, padding: '12px 20px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 16 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              <span style={{ fontSize: 20 }}>⚠️</span>
              <div><div style={{ fontWeight: 700, fontSize: 13, color: '#323130' }}>Column mapping might be wrong</div><div style={{ fontSize: 12, color: '#605E5C', marginTop: 2 }}>{mappingError || 'No orders found. Please check your column mappings.'}</div></div>
            </div>
            <button onClick={() => handleFixMapping(uploadStepOverride || 'table')} style={{ padding: '8px 20px', background: C.amber, color: '#fff', border: 'none', borderRadius: 6, fontWeight: 700, cursor: 'pointer', fontSize: 12 }} onMouseOver={e => e.currentTarget.style.background = C.amberDark} onMouseOut={e => e.currentTarget.style.background = C.amber}>Fix Mapping</button>
          </div>
        )}

        {dataLoaded && (<>
          {/* ─── Filter Bar ─── */}
          <div style={{ background: C.card, borderRadius: 8, padding: '10px 14px', border: `1px solid ${C.border}`, boxShadow: '0 2px 6px rgba(0,0,0,.04)', overflowX: 'auto' }}>
            <div style={{ display: 'flex', flexWrap: 'nowrap', gap: '12px', alignItems: 'end', minWidth: 'max-content', paddingBottom: '4px' }}>
              <div style={{ flexShrink: 0, width: 170 }}>{slicer('case_id', 'Case ID', 'case_ids')}</div>
              <div style={{ flexShrink: 0, width: 130 }}>{slicer('company', 'Company', 'companies')}</div>
              <div style={{ flexShrink: 0, width: 130 }}>{slicer('plant', 'Plant', 'plants')}</div>
              <div style={{ flexShrink: 0, width: 130 }}>{slicer('matnr', 'Material', 'matnrs')}</div>
              <div style={{ flexShrink: 0, width: 130 }}>{slicer('auart', 'Order Type', 'auarts')}</div>
              <div style={{ flexShrink: 0, width: 130 }}>{slicer('kostl', 'Cost Center', 'kostls')}</div>
              <div style={{ flexShrink: 0, width: 130 }}>{slicer('fevor', 'Supervisor', 'fevors')}</div>
              <div style={{ flexShrink: 0, width: 130 }}>{slicer('ernam', 'Creator', 'ernams')}</div>
              <div style={{ flexShrink: 0, width: 130 }}>
                <FilterSelect label="Status" value={selected.status || 'ALL'} options={['ALL', 'Happy Path', 'Deviations']} onChange={v => { logAction('FILTER', `Status = ${v}`); setSelected(p => ({ ...p, status: v })); setCrossFilter(null); }} />
              </div>
              <div style={{ flexShrink: 0, width: 90 }}>{slicer('year', 'Year', 'years')}</div>
              <div style={{ flexShrink: 0, width: 100 }}>{slicer('month', 'Month', 'months')}</div>
              {hasActiveFilters && (
                <button onClick={resetAll} style={{ flexShrink: 0, padding: '5px 12px', borderRadius: 4, border: `1px solid ${C.selectedBorder}`, background: C.selected, color: C.blue700, fontSize: 11, fontWeight: 700, cursor: 'pointer', height: 30, marginBottom: 1 }}>✕ Reset All</button>
              )}
            </div>
          </div>

          {/* ─── KPI Strip ─── */}
          {kpis && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              {/* ── KPI Row 1: 8 cards ── */}
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(8, 1fr)', gap: 6 }}>
                <KpiCard label="Total Order Items" value={kpis.total_orders} tooltip="Unique production order items (AUFNR+POSNR)" />
                <KpiCard label="Orders Planned" value={kpis.orders_planned} tooltip="Orders with Basic Start date (0 when AFKO not uploaded)" />
                <KpiCard label="Orders Confirmed" value={kpis.orders_confirmed} tooltip="Orders with an Operation Confirmation" />
                <KpiCard label="Goods Issued" value={kpis.goods_issued} tooltip="Orders with Goods Issued to Order (BWART 261)" />
                <KpiCard label="FG Receipts" value={kpis.finished_goods} tooltip="Orders with Finished Goods Receipts (BWART 101)" />
                <KpiCard label="PR Created" value={kpis.pr_created} tooltip="Orders linked to a Purchase Requisition" />
                <KpiCard label="PO Created" value={kpis.po_created} tooltip="Orders linked to a Purchase Order" />
                <KpiCard label="GR Posted" value={kpis.gr_postings} tooltip="Orders with a Goods Receipt (EKBE)" />
              </div>
              {/* ── KPI Row 2: 7 cards ── */}
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(7, 1fr)', gap: 6 }}>
                <KpiCard label="Invoices Posted" value={kpis.invoices_posted} tooltip="Orders with an Invoice posted" />
                <KpiCard label="Total Reversals" value={kpis.total_reversals} tooltip="Sum of all reversal events" />
                <KpiCard label="GI Before Confirm" value={kpis.gi_before_confirm} tooltip="GI posted before Order Confirmation — sequence violation" />
                <KpiCard label="FG w/o GI" value={kpis.fg_without_gi} tooltip="Finished Goods received with no Goods Issue" />
                <KpiCard label="GR w/o Invoice" value={kpis.gr_no_invoice} tooltip="Goods Receipts with no Invoice follow-up" />
                <KpiCard label="PO w/o PR" value={kpis.po_without_pr} tooltip="Purchase Orders raised without a prior PR" />
                <KpiCard label="Avg Days (Plan→FG)" value={kpis.avg_completion_days} tooltip="Avg days: Basic Start → FG Receipts (falls back to Confirm→FG when AFKO absent)" />
              </div>
            </div>
          )}

          {/* ─── Tab: Process Mining ─── */}
          {activeTab === 'process' && (
            tabSkeleton ? <TabSkeletonGrid processTab={true} /> :
              <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 10 }}>

                  {/* Process Map */}
                  <ChartCard title="P2I Process Map" subtitle="Gold = happy path (8 steps) · Red = deviation/reversal · Orange dashed = P2P gateway (double-click to explore)" loading={dashboardLoading || tabSkeleton} style={{ height: '100%' }}>
                    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8, flexShrink: 0 }}>
                        <div style={{ display: 'flex', gap: 6 }}>
                          {[['LR', 'Horizontal'], ['TB', 'Vertical']].map(([d, l]) => (
                            <button key={d} onClick={() => setLayoutDir(d)} style={{ fontSize: 11, padding: '4px 10px', borderRadius: 4, border: `1px solid ${layoutDir === d ? C.amber : C.border}`, background: layoutDir === d ? C.amberLight : '#fff', color: layoutDir === d ? C.amberDark : '#605E5C', fontWeight: layoutDir === d ? 700 : 400, cursor: 'pointer' }}>{l}</button>
                          ))}
                        </div>
                        {/* Swim-lane legend */}
                        <div style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
                          {[
                            { color: '#d97706', border: '#b45309', label: 'Happy Path (8 steps)' },
                            { color: '#e74c3c', border: '#c0392b', label: 'Deviations / Reversals' },
                            { color: '#999999', border: '#666666', label: 'P2P Branch' },
                            { color: '#fffbeb', border: '#f59e0b', label: 'P2P Gateway ↗' },
                          ].map(({ color, border, label }) => (
                            <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                              <div style={{ width: 12, height: 12, borderRadius: 2, background: color, border: `1.5px solid ${border}` }} />
                              <span style={{ fontSize: 10, color: '#605E5C' }}>{label}</span>
                            </div>
                          ))}
                        </div>
                        {pmLoading && <span style={{ fontSize: 11, color: C.amber }}>⟳ Updating...</span>}
                        {pmError && <span style={{ fontSize: 11, color: C.red }}>{pmError}</span>}
                      </div>
                      <div style={{ flex: 1, position: 'relative', border: `1px solid ${C.border}`, borderRadius: 6, overflow: 'hidden', minHeight: 450 }}>
                        <div style={{ position: 'absolute', inset: 0 }}>
                          <ReactFlow nodes={rfNodes} edges={rfEdges} onNodesChange={onNodesChange} onEdgesChange={onEdgesChange} nodeTypes={nodeTypes} edgeTypes={edgeTypes} onNodeDoubleClick={onNodeDoubleClick} fitView fitViewOptions={{ padding: 0.2 }} minZoom={0.01} maxZoom={2}>
                            <Background color="#E1DFDD" gap={24} />
                            <Controls />
                            <MiniMap nodeColor={n => HAPPY_PATH_NODES.has(n.id) ? '#d97706' : (REVERSAL_NODES.has(n.id) ? '#e74c3c' : '#999999')} style={{ background: '#F8FAFC', border: `1px solid ${C.border}`, borderRadius: 6 }} />
                          </ReactFlow>
                        </div>
                      </div>
                    </div>
                  </ChartCard>

                  {/* Right column */}
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                    <ChartCard title="Activity Frequency" subtitle="Events per activity type" loading={dashboardLoading || tabSkeleton} skeletonType="bar-horizontal">
                      <ActivityChart data={actData} crossFilter={crossFilter} onSelect={handleSelect} isAnimationActive={false} />
                    </ChartCard>
                    <ChartCard title="Happy Path vs Deviations" subtitle="Full 8-step P2I flow, no reversals (Order Creation, Start, Confirmation, GI, FG, QI)" loading={dashboardLoading || tabSkeleton} highlighted={crossFilter?.type === 'status'} onClear={clearCF} skeletonType="pie">
                      <DonutChart data={happyPathData} dataKey="count" nameKey="status" crossFilterType="status" crossFilter={crossFilter} onSelect={handleSelect} colors={[C.amber, C.teal]} isAnimationActive={false} />
                    </ChartCard>
                    <ChartCard title="Process Bottlenecks" subtitle="Avg & median days between key P2I steps (Order Creation → Start → GI → FG → QI)" loading={dashboardLoading || tabSkeleton} skeletonType="bar-vertical">
                      <BottleneckChart data={bottleneckData} isAnimationActive={false} />
                    </ChartCard>
                  </div>
                </div>

                {/* Second row */}
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 10 }}>
                  <ChartCard title="Lead Time Distribution" subtitle="Days: Order Creation to Quality Inspection (with fallbacks)" loading={dashboardLoading || tabSkeleton} skeletonType="bar-vertical">
                    <LeadTimeChart data={ltData} crossFilter={crossFilter} onSelect={handleSelect} isAnimationActive={false} />
                  </ChartCard>
                  <ChartCard title="Operation Reversals Timeline" subtitle="Orders with Operation Reversed by month" loading={dashboardLoading || tabSkeleton} skeletonType="bar-vertical">
                    <MonthlyChart data={opReversalData} crossFilter={crossFilter} onSelect={handleSelect} isAnimationActive={false} />
                  </ChartCard>
                </div>

                {/* Third row */}
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 10 }}>
                  <ChartCard title="GI Before Confirmation — by Creator" subtitle="Sequence violations: GI posted before Order Confirmation" loading={dashboardLoading || tabSkeleton} highlighted={crossFilter?.type === 'ernam'} onClear={clearCF} skeletonType="bar-horizontal">
                    <ScrollableHBarChart data={giConfirmData} dataKey="count" labelKey="ernam" crossFilter={crossFilter} crossKey="ernam" onSelect={handleSelect} color={C.teal} isAnimationActive={false} />
                  </ChartCard>
                  <ChartCard title="PO Reversal Timeline" subtitle="Monthly trend of PO reversals" loading={dashboardLoading || tabSkeleton} skeletonType="bar-vertical">
                    <MonthlyChart data={poRevTimeline} crossFilter={crossFilter} onSelect={handleSelect} isAnimationActive={false} />
                  </ChartCard>
                </div>

                {/* Case Table */}
                <ChartCard title="Production Order Item List" subtitle="One row per AUFNR+POSNR — click to view event timeline" loading={dashboardLoading || tabSkeleton} skeletonType="timeline">
                  <CaseTable data={caseTableData} events={caseEvents} onSelect={(id) => { logAction('CASE_SELECT', `Selected case: ${id}`); setSelected(p => ({ ...p, case_id: id })); }} selectedId={selected.case_id} />
                </ChartCard>
              </div>
          )}

          {/* ─── Tab: EDA ─── */}
          {activeTab === 'dimensions' && (
            tabSkeleton ? <TabSkeletonGrid cards={6} /> :
              <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 10 }}>
                  <ChartCard title="Orders by Company" subtitle="Unique production orders per company code" loading={dashboardLoading || tabSkeleton} highlighted={crossFilter?.type === 'company'} onClear={clearCF} skeletonType="pie">
                    <DonutChart data={compData} dataKey="count" nameKey="company" crossFilterType="company" crossFilter={crossFilter} onSelect={handleSelect} isAnimationActive={false} />
                  </ChartCard>
                  <ChartCard title="Orders by Plant" subtitle="Production workload per plant (WERKS)" loading={dashboardLoading || tabSkeleton} highlighted={crossFilter?.type === 'plant'} onClear={clearCF} skeletonType="bar-horizontal">
                    <ScrollableHBarChart data={plantData} dataKey="count" labelKey="plant" crossFilter={crossFilter} crossKey="plant" onSelect={handleSelect} color={C.amber} isAnimationActive={false} />
                  </ChartCard>
                  <ChartCard title="Plant Lead Time" subtitle="Avg production lead time per plant (Order Creation → Quality Inspection)" loading={dashboardLoading || tabSkeleton} skeletonType="bar-horizontal">
                    <ScrollableHBarChart data={plantLtData} dataKey="avg_days" labelKey="plant" crossFilter={crossFilter} crossKey="plant" onSelect={handleSelect} color={C.orange} isAnimationActive={false} />
                  </ChartCard>
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 10 }}>
                  <ChartCard title="Top Materials" subtitle="Production orders by material (MATNR)" loading={dashboardLoading || tabSkeleton} highlighted={crossFilter?.type === 'matnr'} onClear={clearCF} skeletonType="bar-horizontal">
                    <ScrollableHBarChart data={matnrData} dataKey="count" labelKey="matnr" crossFilter={crossFilter} crossKey="matnr" onSelect={handleSelect} color={C.amberDark} isAnimationActive={false} />
                  </ChartCard>
                  <ChartCard title="Top Vendors" subtitle="Orders linked to vendors via procurement chain" loading={dashboardLoading || tabSkeleton} highlighted={crossFilter?.type === 'vendor'} onClear={clearCF} skeletonType="bar-horizontal">
                    <ScrollableHBarChart data={vendData} dataKey="count" labelKey="vendor" crossFilter={crossFilter} crossKey="vendor" onSelect={handleSelect} color={C.teal} isAnimationActive={false} />
                  </ChartCard>
                  <ChartCard title="Production Planner Activity" subtitle="Orders created per planner (ERNAM)" loading={dashboardLoading || tabSkeleton} highlighted={crossFilter?.type === 'ernam'} onClear={clearCF} skeletonType="bar-horizontal">
                    <ScrollableHBarChart data={ernamData} dataKey="count" labelKey="ernam" crossFilter={crossFilter} crossKey="ernam" onSelect={handleSelect} color={C.purple} isAnimationActive={false} />
                  </ChartCard>
                </div>
              </div>
          )}
        </>)}
      </div>
    </div>
  );
}
