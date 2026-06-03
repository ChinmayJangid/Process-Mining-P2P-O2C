import React, { useState, useEffect, useCallback, useRef } from 'react';
import ReactFlow, {
  Background, Controls, MiniMap,
  useNodesState, useEdgesState,
  MarkerType, Position,
  getBezierPath,
  EdgeLabelRenderer, BaseEdge, Handle,
} from 'reactflow';
import 'reactflow/dist/style.css';
import 'reactflow/dist/style.css';
import {
  BarChart, Bar, ComposedChart, Line, PieChart, Pie,
  XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Cell, Legend
} from 'recharts';
import { motion, AnimatePresence, useScroll, useSpring } from 'framer-motion';
import html2canvas from 'html2canvas';
import './App.css';

const API = 'http://localhost:8000';

const C = {
  blue700: '#0078D4', teal: '#005A9E', red: '#004B87', purple: '#002D50',
  slate: '#605E5C', bg: '#F0F2F5', card: '#FFFFFF', border: '#E1DFDD',
  orange: '#2B88D8', green: '#85C1F8', selected: '#EFF6FF', selectedBorder: '#0078D4',
  headerBg: '#002D50', mapNodeBg: '#A5C6D9', mapNodeBorder: '#648596', mapEdge: '#8B9CB3',
  jkBlue: '#0057B7'
};
const ACCENT = ['#0078D4', '#005A9E', '#004B87', '#2B88D8', '#58A5EB', '#85C1F8', '#106EBE', '#002D50', '#003E6B', '#1E40AF', '#2563EB', '#3B82F6', '#60A5FA'];



/* ─── LOADING OVERLAY WITH PHASES ──────────────────────────────────────────── */
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
    <div style={{
      position: 'fixed', inset: 0, zIndex: 99999,
      background: 'rgba(27,42,74,0.92)', backdropFilter: 'blur(1px)',
      display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: 36
    }}>
      <style>{`
        @keyframes lo-pulse{0%,100%{opacity:1}50%{opacity:.45}}
      `}</style>

      <img
        src="/logo.png"
        alt="AJALabs Logo"
        style={{
          height: 80,
          objectFit: 'contain',
          animation: 'lo-pulse 1.5s ease-in-out infinite'
        }}
      />
      <div style={{ display: 'flex', gap: 40, alignItems: 'center' }}>
        {phases.map((phase, index) => {
          const isActive = activeStep === phase.num;
          const isDone = activeStep > phase.num;
          const color = isActive || isDone ? '#0078D4' : 'rgba(255,255,255,0.25)';

          return (
            <div key={phase.num} style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 10, position: 'relative' }}>
              {index > 0 && (
                <div style={{
                  position: 'absolute', right: '100%', top: 16, width: 40, height: 2,
                  background: isDone || isActive ? '#0078D4' : 'rgba(255,255,255,0.15)',
                  marginRight: 10, transition: 'all 0.4s ease'
                }} />
              )}
              <div style={{
                width: 32, height: 32, borderRadius: '50%',
                background: isDone ? '#0078D4' : (isActive ? 'rgba(0,120,212,0.1)' : 'transparent'),
                border: `2px solid ${color}`,
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                color: isDone ? '#1B2A4A' : color,
                fontWeight: 'bold', fontSize: 14, zIndex: 2,
                transition: 'all 0.3s ease',
                boxShadow: isActive ? '0 0 12px rgba(0,120,212,0.4)' : 'none'
              }}>
                {isDone ? '✓' : phase.num}
              </div>
              <div style={{
                color: isActive || isDone ? '#fff' : 'rgba(255,255,255,0.4)',
                fontSize: 13, fontWeight: isActive ? 700 : 500,
                transition: 'all 0.3s ease', letterSpacing: 0.5
              }}>
                {phase.name}
              </div>
            </div>
          );
        })}
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 8, width: 400 }}>
        <div style={{ width: '100%', background: 'rgba(255,255,255,.15)', borderRadius: 8, height: 6, overflow: 'hidden' }}>
          <div style={{
            height: '100%', borderRadius: 8, transition: 'width .4s ease',
            background: 'linear-gradient(90deg,#0078D4,#005A9E)',
            width: `${progress}%`, boxShadow: '0 0 12px rgba(0,120,212,.6)'
          }} />
        </div>
        <div style={{ fontSize: 12, color: 'rgba(255,255,255,.6)' }}>
          {label}
        </div>
      </div>
    </div>
  );
};

/* ─── TOOLTIP ──────────────────────────────────────────────────── */
const CustomTooltip = ({ active, payload, nameKey, labelOverride }) => {
  if (!active || !payload?.length || !payload[0]) return null;
  const entry = payload[0].payload || {};
  const name = nameKey ? (entry[nameKey] ?? '') : '';
  const val = payload[0].value;
  const uc = entry?.unique_cases;
  return (
    <div style={{
      background: 'rgba(255,255,255,.98)', border: `1px solid ${C.border}`,
      borderRadius: 6, padding: '8px 14px', boxShadow: '0 4px 12px rgba(0,0,0,.15)',
      fontSize: 12, color: '#323130', maxWidth: 260, zIndex: 9999
    }}>
      {name && <div style={{ fontWeight: 600, marginBottom: 4, color: '#0078D4', wordBreak: 'break-word' }}>{name}</div>}
      <div style={{ color: '#605E5C' }}>
        {labelOverride === 'cases' ? 'Cases:' : 'Events:'}
        &nbsp;<strong style={{ color: '#323130' }}>{val != null ? Number(val).toLocaleString() : 0}</strong>
      </div>
      {uc != null && labelOverride !== 'cases' && (
        <div style={{ color: '#605E5C', marginTop: 2 }}>
          Unique Cases:&nbsp;<strong style={{ color: '#0078D4' }}>{Number(uc).toLocaleString()}</strong>
        </div>
      )}
    </div>
  );
};

/* ─── PROCESS NODE ────────────────────────────────────────────── */
const ProcessNode = React.memo(({ data }) => {
  const freq = data?.frequency || 0;
  const isHappyPath = data?.is_main;

  return (
    <div style={{
      background: isHappyPath ? '#7ebe42' : '#999999',
      border: '2px solid #064f86',
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
      height: '200%',
      position: 'relative',
      zIndex: 10,
    }}>
      {/* Halo Pulse Effect */}
      <motion.div
        style={{
          position: 'absolute',
          inset: -4,
          borderRadius: 12,
          border: '4px solid #064f86',
          zIndex: -1,
          pointerEvents: 'none'
        }}
        initial={{ opacity: 0, scale: 0.95 }}
        whileHover={{
          opacity: [0, 0.6, 0],
          scale: [1, 1.15, 1.35],
          transition: {
            duration: 1.5,
            repeat: Infinity,
            ease: "easeOut"
          }
        }}
      />
      <div style={{
        fontSize: 52,
        fontWeight: 700,
        color: '#102A43',
        lineHeight: 1.3,
        marginBottom: 8,
        width: '200%',
        wordBreak: 'break-word'
      }}>
        {data?.label || ''}
      </div>
      <div style={{ alignSelf: 'center' }}>
        <span style={{
          fontSize: 38,
          fontWeight: 600,
          color: '#005A9E',
          backgroundColor: 'rgba(255,255,255,0.7)',
          borderRadius: 12,
          padding: '3px 12px',
          border: '1px solid rgba(0,0,0,0.08)',
          whiteSpace: 'nowrap'
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

/* ─── FREQ EDGE (Visible Labels) ──────────────────────────────── */
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
  const isSkipped = data?.isSkipped;
  const arcColor = isSkipped ? '#E81123' : '#605E5C';
  const flowColor = isSkipped ? '#E81123' : '#0078D4';

  let edgePath, labelX, labelY;

  if (sweepSide) {
    const mx = (sourceX + targetX) / 2;
    const my = (sourceY + targetY) / 2;

    let cx1, cy1, cx2, cy2;
    if (sweepSide === 'right') {
      cx1 = sourceX + sweepDist; cy1 = sourceY;
      cx2 = targetX + sweepDist; cy2 = targetY;
    } else if (sweepSide === 'left') {
      cx1 = sourceX - sweepDist; cy1 = sourceY;
      cx2 = targetX - sweepDist; cy2 = targetY;
    } else if (sweepSide === 'top') {
      cx1 = sourceX; cy1 = sourceY - sweepDist;
      cx2 = targetX; cy2 = targetY - sweepDist;
    } else {
      cx1 = sourceX; cy1 = sourceY + sweepDist;
      cx2 = targetX; cy2 = targetY + sweepDist;
    }
    edgePath = `M ${sourceX} ${sourceY} C ${cx1} ${cy1}, ${cx2} ${cy2}, ${targetX} ${targetY}`;

    const t = 0.65;
    labelX = cubicBezierPoint(sourceX, cx1, cx2, targetX, t);
    labelY = cubicBezierPoint(sourceY, cy1, cy2, targetY, t);

    const tx = cubicBezierPoint(sourceX, cx1, cx2, targetX, t + 0.01) - labelX;
    const ty = cubicBezierPoint(sourceY, cy1, cy2, targetY, t + 0.01) - labelY;
    const len = Math.sqrt(tx * tx + ty * ty) || 1;
    const perpX = -ty / len;
    const perpY = tx / len;
    labelX += perpX * 14;
    labelY += perpY * 14;

  } else {
    [edgePath, labelX, labelY] = getBezierPath({
      sourceX, sourceY, sourcePosition,
      targetX, targetY, targetPosition,
      curvature,
    });

    labelX = sourceX + (labelX - sourceX) * 1.3;
    labelY = sourceY + (labelY - sourceY) * 1.3;

    const clamp = (v, a, b) => Math.min(Math.max(v, Math.min(a, b)), Math.max(a, b));
    labelX = clamp(labelX, sourceX, targetX);
    labelY = clamp(labelY, sourceY, targetY);

    const dx = targetX - sourceX;
    const dy = targetY - sourceY;
    const len = Math.sqrt(dx * dx + dy * dy) || 1;
    labelX += (-dy / len) * 14;
    labelY += (dx / len) * 14;
  }

  return (
    <>
      <BaseEdge id={id} path={edgePath} markerEnd={markerEnd}
        style={{ ...style, stroke: arcColor, strokeWidth: width, opacity: .85 }} />

      <path
        d={edgePath}
        fill="none"
        stroke={flowColor}
        strokeWidth={Math.max(4, width / 1.5)}
        strokeDasharray="1 20"
        strokeLinecap="round"
        style={{
          opacity: 0.6,
          animation: `cometFlow ${Math.max(4, 10 - (freq / max) * 6)}s linear infinite`
        }}
      />

      {/* Moving Arrow Stream Animation */}
      {[0, 1, 2].map((i) => {
        const duration = Math.max(4, 10 - (freq / max) * 6);
        return (
          <path
            key={i}
            d="M -8,-6 L 8,0 L -8,6 Z"
            fill={flowColor}
            style={{
              opacity: 0,
            }}
          >
            <animateMotion
              dur={`${duration}s`}
              repeatCount="indefinite"
              path={edgePath}
              rotate="auto"
              begin={`${i * (duration / 3)}s`}
            />
            <animate
              attributeName="opacity"
              values="0;1;1;0"
              keyTimes="0;0.1;0.9;1"
              dur={`${duration}s`}
              repeatCount="indefinite"
              begin={`${i * (duration / 3)}s`}
            />
          </path>
        );
      })}

      {freq > 0 && (
        <EdgeLabelRenderer>
          <div style={{
            position: 'absolute',
            transform: `translate(-50%,-50%) translate(${labelX}px,${labelY}px)`,
            pointerEvents: 'all', zIndex: 100,
            display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 2
          }}>
            <div style={{
              fontSize: 25, fontWeight: 700, color: '#323130', background: 'rgba(255,255,255,0.95)',
              border: '1px solid #E1DFDD', padding: '1px 6px', borderRadius: 4,
              boxShadow: '0 2px 4px rgba(0,0,0,0.12)'
            }}>
              {Number(freq).toLocaleString()}
            </div>
            {data?.avg_days != null && (
              <div style={{
                fontSize: 25, color: '#605E5C', background: 'rgba(255,255,255,.9)',
                padding: '0 4px', borderRadius: 2
              }}>
                {data.avg_days}d
              </div>
            )}
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  );
});

const nodeTypes = { processNode: ProcessNode };
const edgeTypes = { freqEdge: FreqEdge };

/* ─── HAPPY PATH ORDER ─────────────────────────────────────────── */
const HAPPY_PATH = [
  "PR Creation", "PR Release Date", "PO Creation",
  "PO Date", "GR Posting", "Invoice Posting"
];
const HAPPY_IDX = Object.fromEntries(HAPPY_PATH.map((n, i) => [n, i]));

const SIDE_ABOVE_LR = new Set(["PR Reversal", "PR Reversal (Post-PO)", "PO Reversal", "GR Reversal", "Invoice Reversal Date"]);
const SIDE_BELOW_LR = new Set(["PO Reversal (Post-GR)", "GR Reversal (Post-Inv)"]);
const SIDE_LEFT_TB = new Set(["PR Reversal", "PO Reversal", "GR Reversal", "Invoice Reversal Date"]);
const SIDE_RIGHT_TB = new Set(["PR Reversal (Post-PO)", "PO Reversal (Post-GR)", "GR Reversal (Post-Inv)"]);

const classifyEdge = (src, tgt, sPos, tPos, dir) => {
  const sIdx = HAPPY_IDX[src];
  const tIdx = HAPPY_IDX[tgt];
  const sIsHappy = sIdx !== undefined;
  const tIsHappy = tIdx !== undefined;

  if (sIsHappy && tIsHappy) {
    const steps = tIdx - sIdx;
    if (steps === 1) {
      if (dir === 'LR') return { sh: 'right-s', th: 'left-t', curvature: 0.1 };
      else return { sh: 'bottom-s', th: 'top-t', curvature: 0.1 };
    }
    if (steps > 1) {
      const sweepDist = 150 + steps * 120;
      if (dir === 'LR') return { sh: 'top-s', th: 'top-t', sweepSide: 'top', sweepDist };
      else return { sh: 'right-s', th: 'right-t', sweepSide: 'right', sweepDist };
    }
    if (steps < 0) {
      const sweepDist = 150 + Math.abs(steps) * 120;
      if (dir === 'LR') return { sh: 'bottom-s', th: 'bottom-t', sweepSide: 'bottom', sweepDist };
      else return { sh: 'left-s', th: 'left-t', sweepSide: 'left', sweepDist };
    }
  }

  if (dir === 'LR') {
    const srcAbove = SIDE_ABOVE_LR.has(src);
    const tgtAbove = SIDE_ABOVE_LR.has(tgt);
    const srcBelow = SIDE_BELOW_LR.has(src);
    const tgtBelow = SIDE_BELOW_LR.has(tgt);

    if (sIsHappy && tgtAbove) return { sh: 'top-s', th: 'bottom-t', curvature: 0.5 };
    if (srcAbove && tIsHappy) return { sh: 'bottom-s', th: 'top-t', curvature: 0.5 };
    if (sIsHappy && tgtBelow) return { sh: 'bottom-s', th: 'top-t', curvature: 0.5 };
    if (srcBelow && tIsHappy) return { sh: 'top-s', th: 'bottom-t', curvature: 0.5 };
    if ((srcAbove || srcBelow) && (tgtAbove || tgtBelow))
      return { sh: 'right-s', th: 'left-t', curvature: 0.4 };
  } else {
    const srcLeft = SIDE_LEFT_TB.has(src);
    const tgtLeft = SIDE_LEFT_TB.has(tgt);
    const srcRight = SIDE_RIGHT_TB.has(src);
    const tgtRight = SIDE_RIGHT_TB.has(tgt);

    if (sIsHappy && tgtLeft) return { sh: 'left-s', th: 'right-t', curvature: 0.5 };
    if (srcLeft && tIsHappy) return { sh: 'right-s', th: 'left-t', curvature: 0.5 };
    if (sIsHappy && tgtRight) return { sh: 'right-s', th: 'left-t', curvature: 0.5 };
    if (srcRight && tIsHappy) return { sh: 'left-s', th: 'right-t', curvature: 0.5 };
    if ((srcLeft || srcRight) && (tgtLeft || tgtRight))
      return { sh: 'bottom-s', th: 'top-t', curvature: 0.4 };
    if (srcLeft && tIsHappy)
      return { sh: 'right-s', th: 'right-t', sweepSide: 'right', sweepDist: 160 };
  }

  const dx = tPos.x - sPos.x;
  const dy = tPos.y - sPos.y;
  if (dir === 'LR') {
    if (Math.abs(dy) < 80) {
      if (dx > 0) return { sh: 'right-s', th: 'left-t', curvature: 0.3 };
      else return { sh: 'bottom-s', th: 'bottom-t', curvature: 0.5 };
    }
    if (dy > 0) return { sh: 'bottom-s', th: 'top-t', curvature: 0.4 };
    else return { sh: 'top-s', th: 'bottom-t', curvature: 0.4 };
  } else {
    if (Math.abs(dx) < 80) {
      if (dy > 0) return { sh: 'bottom-s', th: 'top-t', curvature: 0.3 };
      else return { sh: 'right-s', th: 'right-t', curvature: 0.5 };
    }
    if (dx > 0) return { sh: 'right-s', th: 'left-t', curvature: 0.4 };
    else return { sh: 'left-s', th: 'right-t', curvature: 0.4 };
  }
};

const buildFlowMap = (bNodes, bEdges, setRfNodes, setRfEdges, dir) => {
  const mxF = Math.max(1, ...(bNodes || []).map(n => n.frequency || 0));
  const mxE = Math.max(1, ...(bEdges || []).map(e => e.frequency || 0));

  const nodes = (bNodes || []).map((n, index) => {
    const basePos = dir === 'LR' ? (n.position_h || { x: 0, y: 0 }) : (n.position_v || { x: 0, y: 0 });
    const pos = {
      x: basePos.x + (index * 0.1),
      y: basePos.y + (index * 0.1)
    };
    return {
      id: n.id, type: 'processNode',
      position: pos,
      data: {
        label: n.label,
        is_main: n.is_main,
        frequency: n.frequency || 0,
        maxFreq: mxF
      },
    };
  });

  const edges = (bEdges || []).map(e => {
    const sN = nodes.find(n => n.id === e.source);
    const tN = nodes.find(n => n.id === e.target);
    if (!sN || !tN) return null;

    const { sh, th, curvature, sweepSide, sweepDist } = classifyEdge(
      e.source, e.target, sN.position, tN.position, dir
    );

    const sIdx = HAPPY_IDX[e.source];
    const tIdx = HAPPY_IDX[e.target];
    const isSkipped = sIdx !== undefined && tIdx !== undefined && (tIdx !== sIdx + 1);

    return {
      id: e.id || `${e.source}--${e.target}`,
      source: e.source, target: e.target,
      sourceHandle: sh, targetHandle: th,
      type: 'freqEdge',
      markerEnd: { type: MarkerType.ArrowClosed, color: isSkipped ? '#E81123' : '#605E5C', width: 15, height: 15 },
      data: { frequency: e.frequency, avg_days: e.avg_days, maxFreq: mxE, curvature, sweepSide, sweepDist, isSkipped },
    };
  }).filter(Boolean);

  setRfNodes(nodes);
  setRfEdges(edges);
};

/* ─── HELPERS ──────────────────────────────────────────────────── */
const VALID_KEYS = new Set(['company', 'bsart', 'matkl', 'vendor', 'plant', 'purch_group',
  'case_id', 'month', 'activity', 'quarter', 'lead_time', 'events', 'ernam', 'status', 'sod', 'start_date', 'end_date']);
const qs = (params) => {
  const p = Object.entries(params).filter(([k, v]) => VALID_KEYS.has(k) && v && v !== 'ALL');
  return p.length ? '?' + p.map(([k, v]) => `${k}=${encodeURIComponent(v)}`).join('&') : '';
}
const CROSS_TO_PARAM = {
  company: 'company', bsart: 'bsart', matkl: 'matkl', vendor: 'vendor', plant: 'plant',
  activity: 'activity', month: 'month', quarter: 'quarter',
  case_id: 'case_id', purch_group: 'purch_group', lead_time: 'lead_time', events: 'events', ernam: 'ernam',
  status: 'status', sod: 'sod'
};

const Skeleton = ({ width = '100%', height = '20px', borderRadius = 4, style }) => (
  <div className="skeleton-shimmer" style={{ width, height, borderRadius, ...style }} />
);

const ChartSkeleton = () => (
  <div style={{ display: 'flex', flexDirection: 'column', gap: 12, padding: 10 }}>
    <Skeleton width="60%" height="24px" />
    <Skeleton width="90%" height="150px" />
    <div style={{ display: 'flex', gap: 10 }}>
      <Skeleton width="30%" height="16px" />
      <Skeleton width="30%" height="16px" />
    </div>
  </div>
);

/* ─── TAB SWITCH SKELETON (YouTube-style) ─────────────────────── */
const TabSkeletonCard = ({ delay = 0, tall = false }) => (
  <div className="tab-skeleton-card" style={{ animationDelay: `${delay}s` }}>
    <div className="tab-skeleton-title" style={{ animationDelay: `${delay + 0.05}s` }} />
    <div className="tab-skeleton-sub" style={{ animationDelay: `${delay + 0.1}s` }} />
    <div className="tab-skeleton-chart" style={{ height: tall ? 220 : 160, animationDelay: `${delay + 0.15}s` }} />
  </div>
);

const TabSkeletonGrid = ({ cards = 6, processTab = false }) => {
  if (processTab) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
        <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 10 }}>
          <div className="tab-skeleton-card" style={{ height: 915 }}>
            <div className="tab-skeleton-title" />
            <div className="tab-skeleton-sub" />
            <div className="tab-skeleton-chart" style={{ flex: 1, height: 'calc(100% - 60px)' }} />
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
            {[0, 1, 2].map(i => <TabSkeletonCard key={i} delay={i * 0.08} tall />)}
          </div>
        </div>
        <div className="tab-skeleton-grid" style={{ gridTemplateColumns: 'repeat(2, 1fr)' }}>
          {[0, 1, 2, 3, 4, 5].map(i => <TabSkeletonCard key={i} delay={i * 0.06} />)}
        </div>
      </div>
    );
  }
  return (
    <div className="tab-skeleton-grid">
      {Array.from({ length: cards }).map((_, i) => (
        <TabSkeletonCard key={i} delay={i * 0.07} tall={i >= cards - 2} />
      ))}
    </div>
  );
};

const Empty = () => (
  <div style={{
    height: 90, display: 'flex', alignItems: 'center', justifyContent: 'center',
    color: C.slate, fontSize: 12
  }}>No data available</div>
);

/* ─── KPI CARD ─────────────────────────────────────────────────── */
const KpiCard = React.memo(({ label, value, color, highlighted, onClick, tooltip }) => {
  const [hover, setHover] = useState(false);
  const bColor = hover ? 'rgba(0,120,212,0.5)' : (highlighted ? C.selectedBorder : 'transparent');
  const bWidth = '1.5px';

  return (
    <div onClick={onClick} onMouseEnter={() => setHover(true)} onMouseLeave={() => setHover(false)} style={{
      background: highlighted ? C.selected : C.card, borderRadius: 6, padding: '10px 14px',
      borderTop: `${bWidth} solid ${bColor}`, borderRight: `${bWidth} solid ${bColor}`,
      borderBottom: `${bWidth} solid ${bColor}`, borderLeft: `4px solid #0078D4`,
      boxShadow: hover ? '0 6px 16px rgba(0,120,212,.15)' : '0 2px 6px rgba(0,0,0,.05)',
      transition: 'all .2s', cursor: onClick ? 'pointer' : 'default', minWidth: 0, position: 'relative',
      display: 'flex', flexDirection: 'column', alignItems: 'center', textAlign: 'center',
      transform: hover ? 'translateY(-3px)' : 'none', boxSizing: 'border-box',
      zIndex: hover ? 50 : 1
    }}>
      <div style={{
        fontSize: 10, fontWeight: 600, color: "#0078D4", textTransform: 'uppercase',
        letterSpacing: .5, marginBottom: 2
      }}>{label}</div>
      <div style={{ fontSize: 20, fontWeight: 600, color: '#000000', lineHeight: 1 }}>
        {value != null ? Number(value).toLocaleString() : '—'}
      </div>
      {hover && tooltip && (
        <div style={{
          position: 'absolute', top: '100%', left: 0, marginTop: 4,
          background: '#fff', border: `1px solid ${C.border}`, borderRadius: 4,
          padding: '6px 10px', boxShadow: '0 4px 12px rgba(0,0,0,.15)',
          fontSize: 11, color: '#323130', zIndex: 100, whiteSpace: 'nowrap', textAlign: 'left'
        }}>
          {tooltip}
        </div>
      )}
    </div>
  );
});

const ConfKpiCard = React.memo(({ label, value, color, sub, tooltip, onClick, highlighted }) => {
  const [hover, setHover] = useState(false);
  const bColor = hover ? 'rgba(0,120,212,0.5)' : (highlighted ? C.selectedBorder : 'transparent');
  const bWidth = '1.5px';

  return (
    <div onClick={onClick} onMouseEnter={() => setHover(true)} onMouseLeave={() => setHover(false)} style={{
      background: highlighted ? C.selected : C.card, borderRadius: 6, padding: '10px 14px',
      borderTop: `${bWidth} solid ${bColor}`, borderRight: `${bWidth} solid ${bColor}`,
      borderBottom: `${bWidth} solid ${bColor}`, borderLeft: `4px solid #0078D4`,
      boxShadow: hover ? '0 6px 16px rgba(0,120,212,.15)' : '0 2px 6px rgba(0,0,0,.05)',
      transition: 'all .2s', cursor: onClick ? 'pointer' : 'default', minWidth: 0, position: 'relative',
      display: 'flex', flexDirection: 'column', alignItems: 'center', textAlign: 'center',
      transform: hover ? 'translateY(-3px)' : 'none', boxSizing: 'border-box',
      zIndex: hover ? 50 : 1
    }}>
      <div style={{
        fontSize: 10, fontWeight: 600, color: "#0078D4", textTransform: 'uppercase',
        letterSpacing: .5, marginBottom: 2
      }}>{label}</div>
      <div style={{ fontSize: 20, fontWeight: 600, color: '#000000', lineHeight: 1 }}>
        {value != null ? Number(value).toLocaleString() : '—'}
      </div>
      {sub && <div style={{ fontSize: 10, color: C.slate, marginTop: 3 }}>{sub}</div>}
      {hover && tooltip && (
        <div style={{
          position: 'absolute', top: '100%', left: 0, marginTop: 4,
          background: '#fff', border: `1px solid ${C.border}`, borderRadius: 4,
          padding: '6px 10px', boxShadow: '0 4px 12px rgba(0,0,0,.15)',
          fontSize: 11, color: '#323130', zIndex: 100, whiteSpace: 'nowrap', textAlign: 'left'
        }}>
          {tooltip}
        </div>
      )}
    </div>
  );
});

/* ─── SEARCHABLE SELECT COMPONENT ─────────────────────────────── */
const SearchableSelect = ({ label, value, options, onChange, style }) => {
  const [isOpen, setIsOpen] = useState(false);
  const [search, setSearch] = useState('');
  const dropdownRef = useRef(null);

  useEffect(() => {
    const handleClickOutside = (event) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
        setIsOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const filteredOptions = React.useMemo(() => {
    const list = ['ALL', ...options];
    if (!search) return list;
    const s = search.toLowerCase();
    return list.filter(opt => String(opt).toLowerCase().includes(s));
  }, [options, search]);

  const displayedOptions = filteredOptions.slice(0, 100);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 3, flex: 1, minWidth: '150px', position: 'relative', ...style }} ref={dropdownRef}>
      <label style={{
        fontSize: 10, fontWeight: 700, color: '#323130',
        textTransform: 'uppercase', letterSpacing: .4
      }}>{label}</label>

      <div
        onClick={() => setIsOpen(!isOpen)}
        style={{
          fontSize: 12, padding: '5px 8px', borderRadius: 4, width: '100%',
          border: value && value !== 'ALL' ? `1.5px solid ${C.blue700}` : `1px solid ${C.border}`,
          background: value && value !== 'ALL' ? '#EFF6FF' : C.card,
          color: '#323130', cursor: 'pointer', fontWeight: value && value !== 'ALL' ? 700 : 'normal',
          display: 'flex', justifyContent: 'space-between', alignItems: 'center'
        }}
      >
        <span style={{ whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{value || 'ALL'}</span>
        <span style={{ fontSize: 10, opacity: 0.6 }}>▼</span>
      </div>

      {isOpen && (
        <div style={{
          position: 'absolute', top: '100%', left: 0, right: 0, zIndex: 1000,
          background: '#fff', border: `1px solid ${C.border}`, borderRadius: 4,
          boxShadow: '0 4px 12px rgba(0,0,0,0.15)', maxHeight: 200, overflowY: 'auto',
          marginTop: 2
        }}>
          <input
            type="text"
            placeholder="Search..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            style={{
              width: '100%', padding: '6px', border: 'none', borderBottom: `1px solid ${C.border}`,
              fontSize: 12, outline: 'none'
            }}
            onClick={(e) => e.stopPropagation()}
            autoFocus
          />
          {displayedOptions.map((opt, i) => (
            <div
              key={i}
              onClick={() => { onChange(opt); setIsOpen(false); setSearch(''); }}
              style={{
                padding: '6px 8px', fontSize: 12, cursor: 'pointer',
                background: value === opt ? '#EFF6FF' : '#fff',
                color: value === opt ? C.blue700 : '#323130',
                borderLeft: value === opt ? `3px solid ${C.blue700}` : '3px solid transparent'
              }}
              onMouseEnter={(e) => { if (value !== opt) e.currentTarget.style.background = '#F3F2F1'; }}
              onMouseLeave={(e) => { if (value !== opt) e.currentTarget.style.background = '#fff'; }}
            >
              {opt}
            </div>
          ))}
          {filteredOptions.length > 100 && (
            <div style={{ padding: '8px', fontSize: 10, color: '#8A8886', textAlign: 'center', background: '#f9f9f9', borderTop: `1px solid ${C.border}` }}>
              Showing first 100 of {filteredOptions.length} results. Use search to narrow down.
            </div>
          )}
          {filteredOptions.length === 0 && (
            <div style={{ padding: '12px', fontSize: 11, color: '#8A8886', textAlign: 'center' }}>
              No matches found
            </div>
          )}
        </div>
      )}
    </div>
  );
};

/* ─── STANDARD SELECT ──────────────────────────────────────────── */
const FilterSelect = ({ label, value, options, onChange, style }) => (
  <div style={{ display: 'flex', flexDirection: 'column', gap: 3, flex: 1, minWidth: '150px', ...style }}>
    <label style={{
      fontSize: 10, fontWeight: 700, color: '#323130',
      textTransform: 'uppercase', letterSpacing: .4
    }}>{label}</label>
    <select value={value} onChange={e => onChange(e.target.value)} style={{
      fontSize: 12, padding: '5px 8px', borderRadius: 4, width: '100%',
      border: value && value !== 'ALL' ? `1.5px solid ${C.blue700}` : `1px solid ${C.border}`,
      background: value && value !== 'ALL' ? '#EFF6FF' : C.card,
      color: '#323130', outline: 'none', cursor: 'pointer',
      fontWeight: value && value !== 'ALL' ? 700 : 'normal',
    }}>
      {(Array.isArray(options) ? options : ['ALL']).map(o => (
        <option key={o} value={o}>{o}</option>
      ))}
    </select>
  </div>
);

const DateRangeFilter = ({ label, fromValue, toValue, onFromChange, onToChange, minDate, maxDate }) => (
  <div style={{ display: 'flex', flexDirection: 'column', gap: 3, flex: 1.5, minWidth: '220px' }}>
    <label style={{
      fontSize: 10, fontWeight: 700, color: '#323130',
      textTransform: 'uppercase', letterSpacing: .4
    }}>{label}</label>
    <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
      <input
        type="date"
        value={fromValue || ''}
        min={minDate || ''}
        max={maxDate || ''}
        onChange={e => onFromChange(e.target.value)}
        style={{
          fontSize: 11, padding: '4px 6px', borderRadius: 4, flex: 1,
          border: fromValue ? `1.5px solid ${C.blue700}` : `1px solid ${C.border}`,
          background: fromValue ? '#EFF6FF' : C.card,
          color: '#323130', outline: 'none', cursor: 'pointer',
          fontWeight: fromValue ? 700 : 'normal',
        }}
      />
      <span style={{ fontSize: 11, color: '#605E5C' }}>to</span>
      <input
        type="date"
        value={toValue || ''}
        min={minDate || ''}
        max={maxDate || ''}
        onChange={e => onToChange(e.target.value)}
        style={{
          fontSize: 11, padding: '4px 6px', borderRadius: 4, flex: 1,
          border: toValue ? `1.5px solid ${C.blue700}` : `1px solid ${C.border}`,
          background: toValue ? '#EFF6FF' : C.card,
          color: '#323130', outline: 'none', cursor: 'pointer',
          fontWeight: toValue ? 700 : 'normal',
        }}
      />
    </div>
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
  // Default to bar-vertical
  return (
    <div style={{ flex: 1, display: 'flex', alignItems: 'flex-end', gap: 8, marginTop: 10 }}>
      {[35, 65, 40, 80, 55, 90].map((h, i) => (
        <div key={i} className="chart-skeleton-bar" style={{ flex: 1, height: `${h}%` }} />
      ))}
    </div>
  );
};

/* ─── CHART CARD ───────────────────────────────────────────────── */
const ChartCard = React.memo(({ title, subtitle, children, highlighted, onClear, style = {}, loading = false, skeletonType = 'bar-vertical' }) => (
  <div style={{
    background: C.card, borderRadius: 8, padding: '12px 14px',
    border: highlighted ? `1.5px solid ${C.selectedBorder}` : `1px solid ${C.border}`,
    boxShadow: '0 2px 8px rgba(0,0,0,.05)', transition: 'all .2s',
    display: 'flex', flexDirection: 'column', minWidth: 0, ...style
  }}>

    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 8 }}>
      <div>
        <div style={{ fontSize: 13, fontWeight: 700, color: '#323130' }}>{title}</div>
        {subtitle && <div style={{ fontSize: 10, color: '#8A8886', marginTop: 2 }}>{subtitle}</div>}
      </div>
      {highlighted && onClear && (
        <button onClick={onClear} style={{
          fontSize: 11, color: '#fff', background: C.blue700,
          border: 'none', borderRadius: 4, padding: '3px 9px', cursor: 'pointer', fontWeight: 600, flexShrink: 0
        }}>
          Clear
        </button>
      )}
    </div>

    <div style={{ flex: 1, minHeight: 0, position: 'relative' }}>
      {loading && (
        <div className="chart-skeleton-container" style={{
          position: 'absolute', inset: 0, zIndex: 10,
          background: C.card,
          display: 'flex', flexDirection: 'column', gap: 12, padding: 10
        }}>
          <div className="chart-skeleton-bar" style={{ width: '40%', height: 14 }} />
          {renderSkeleton(skeletonType)}
        </div>
      )}
      <div style={{ opacity: loading ? 0 : 1, transition: 'opacity 0.3s', height: '100%' }}>
        {children}
      </div>
    </div>
  </div>
));

/* ─── CASE TABLE & EVENTS LINE ITEMS ──────────────────────────── */
const CaseTable = React.memo(({ data, events, onSelect, selectedId }) => {
  if (selectedId !== 'ALL' && selectedId != null) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', height: '320px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8, paddingBottom: 8, borderBottom: `1px solid ${C.border}` }}>
          <div>
            <span style={{ fontSize: 11, color: C.slate }}>Event Log for Case: </span>
            <strong style={{ fontSize: 13, color: '#323130' }}>{selectedId}</strong>
          </div>
          <button
            onClick={() => onSelect('ALL')}
            style={{ fontSize: 11, background: C.blue700, color: '#fff', border: 'none', padding: '4px 12px', borderRadius: 4, cursor: 'pointer', fontWeight: 600 }}>
            Back to Case List
          </button>
        </div>
        <div style={{ overflowY: 'auto', flex: 1 }}>
          {(!events || events.length === 0) ? <Empty /> : (
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '11px' }}>
              <thead style={{ position: 'sticky', top: 0, background: '#F0F2F5', zIndex: 1 }}>
                <tr>
                  <th style={{ padding: '8px', textAlign: 'left', borderBottom: `1px solid ${C.border}`, color: C.slate }}>Activity</th>
                  <th style={{ padding: '8px', textAlign: 'left', borderBottom: `1px solid ${C.border}`, color: C.slate }}>Timestamp</th>
                  <th style={{ padding: '8px', textAlign: 'left', borderBottom: `1px solid ${C.border}`, color: C.slate }}>User (ERNAM)</th>
                </tr>
              </thead>
              <tbody>
                {events.map((e, i) => (
                  <tr key={i} style={{ borderBottom: `1px solid ${C.border}`, background: i % 2 === 0 ? '#fff' : '#fafafa' }}>
                    <td style={{ padding: '6px 8px', color: '#323130', fontWeight: 600 }}>{e.Activity}</td>
                    <td style={{ padding: '6px 8px', color: '#605E5C' }}>{e.Timestamp}</td>
                    <td style={{ padding: '6px 8px', color: '#605E5C' }}>{e.User}</td>
                  </tr>
                ))}
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
            <th style={{ padding: '8px', textAlign: 'left', borderBottom: `1px solid ${C.border}`, color: C.slate }}>Case ID</th>
            <th style={{ padding: '8px', textAlign: 'left', borderBottom: `1px solid ${C.border}`, color: C.slate }}>Start Date</th>
            <th style={{ padding: '8px', textAlign: 'left', borderBottom: `1px solid ${C.border}`, color: C.slate }}>End Date</th>
          </tr>
        </thead>
        <tbody>
          {data.map((row, i) => (
            <tr
              key={i}
              onClick={() => onSelect(row.case_id)}
              style={{
                borderBottom: `1px solid ${C.border}`,
                background: row.case_id === selectedId ? '#EFF6FF' : (i % 2 === 0 ? '#fff' : '#fafafa'),
                cursor: 'pointer',
                transition: 'background 0.2s',
                borderLeft: row.case_id === selectedId ? `4px solid ${C.blue700}` : '4px solid transparent'
              }}
            >
              <td style={{ padding: '6px 8px', color: '#323130', fontWeight: 600 }}>{row.case_id}</td>
              <td style={{ padding: '6px 8px', color: '#605E5C' }}>{row.start_date}</td>
              <td style={{ padding: '6px 8px', color: '#605E5C' }}>{row.end_date}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
});

const CaseTableEda = React.memo(({ data, events, onSelect, selectedId }) => {
  const [limit, setLimit] = useState(100);
  const [searchQuery, setSearchQuery] = useState('');

  // Reset limit when search query changes
  useEffect(() => {
    setLimit(100);
  }, [searchQuery]);

  // ── 1. Calculate non-blank/filled fields for custom sorting ─────────────────
  const getFilledScore = React.useCallback((row) => {
    let score = 0;
    const targetCols = ["PO Qty", "Netpr", "Netwr", "GR Quantity", "GR Doc Amount", "IR Quantity", "IR Doc Amount"];
    targetCols.forEach(col => {
      const v = row[col];
      if (v != null && v !== '' && v !== 0 && v !== '0') {
        score += 1;
      }
    });
    return score;
  }, []);

  // ── 2. Filter & Sort cases: filled first, blank after ────────────────────────
  const processedData = React.useMemo(() => {
    let list = [...data];
    if (searchQuery.trim()) {
      const q = searchQuery.toLowerCase();
      list = list.filter(row => String(row.case_id).toLowerCase().includes(q));
    }

    // Sort logic: higher score (more filled columns) comes first
    return list.sort((a, b) => {
      const scoreA = getFilledScore(a);
      const scoreB = getFilledScore(b);
      if (scoreA !== scoreB) {
        return scoreB - scoreA; // descending order of filled columns
      }
      // Stable sorting fallback on case_id
      return String(b.case_id).localeCompare(String(a.case_id));
    });
  }, [data, searchQuery, getFilledScore]);

  const visibleData = React.useMemo(() => {
    return processedData.slice(0, limit);
  }, [processedData, limit]);

  const handleScroll = (e) => {
    const { scrollTop, scrollHeight, clientHeight } = e.target;
    // Load 100 more rows when user scrolls near the bottom (within 100px)
    if (scrollHeight - scrollTop - clientHeight < 100) {
      if (limit < processedData.length) {
        setLimit(prev => prev + 100);
      }
    }
  };

  const formatQty = (v) => (v != null && v !== '') ? Number(v).toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 3 }) : '—';
  const formatAmt = (v) => (v != null && v !== '') ? Number(v).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : '—';

  // ── Conditional Return for Selected Case Event Log (placed at the bottom to avoid hook violation) ──
  if (selectedId !== 'ALL' && selectedId != null) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', height: '360px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8, paddingBottom: 8, borderBottom: `1px solid ${C.border}` }}>
          <div>
            <span style={{ fontSize: 11, color: C.slate }}>Event Log for Case: </span>
            <strong style={{ fontSize: 13, color: '#323130' }}>{selectedId}</strong>
          </div>
          <button
            onClick={() => onSelect('ALL')}
            style={{ fontSize: 11, background: C.blue700, color: '#fff', border: 'none', padding: '4px 12px', borderRadius: 4, cursor: 'pointer', fontWeight: 600 }}>
            Back to Case List
          </button>
        </div>
        <div style={{ overflowY: 'auto', flex: 1 }}>
          {(!events || events.length === 0) ? <Empty /> : (
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '11px' }}>
              <thead style={{ position: 'sticky', top: 0, background: '#F0F2F5', zIndex: 1 }}>
                <tr>
                  <th style={{ padding: '8px', textAlign: 'left', borderBottom: `1px solid ${C.border}`, color: C.slate }}>Activity</th>
                  <th style={{ padding: '8px', textAlign: 'left', borderBottom: `1px solid ${C.border}`, color: C.slate }}>Timestamp</th>
                  <th style={{ padding: '8px', textAlign: 'left', borderBottom: `1px solid ${C.border}`, color: C.slate }}>User (ERNAM)</th>
                </tr>
              </thead>
              <tbody>
                {events.map((e, i) => (
                  <tr key={i} style={{ borderBottom: `1px solid ${C.border}`, background: i % 2 === 0 ? '#fff' : '#fafafa' }}>
                    <td style={{ padding: '6px 8px', color: '#323130', fontWeight: 600 }}>{e.Activity}</td>
                    <td style={{ padding: '6px 8px', color: '#605E5C' }}>{e.Timestamp}</td>
                    <td style={{ padding: '6px 8px', color: '#605E5C' }}>{e.User}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    );
  }

  if (!Array.isArray(data) || !data.length) return <Empty />;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      {/* Search and stats bar */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 16 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, background: '#fff', border: `1px solid ${C.border}`, borderRadius: 4, padding: '4px 8px', width: '260px' }}>
          <span style={{ fontSize: 12, opacity: 0.6 }}>🔍</span>
          <input
            type="text"
            placeholder="Search Case ID..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            style={{ border: 'none', outline: 'none', fontSize: 11, width: '100%' }}
          />
        </div>
        <div style={{ fontSize: 11, color: C.slate }}>
          Showing top <strong>{Math.min(processedData.length, limit)}</strong> of <strong>{processedData.length}</strong> cases (Scroll down to load more)
        </div>
      </div>

      {/* Scrollable Container with handleScroll */}
      <div
        onScroll={handleScroll}
        style={{ overflowX: 'auto', maxHeight: '360px', overflowY: 'auto', border: `1px solid ${C.border}`, borderRadius: '6px' }}
      >
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '11px' }}>
          <thead style={{ position: 'sticky', top: 0, background: '#F0F2F5', zIndex: 2 }}>
            <tr>
              <th style={{ padding: '8px', textAlign: 'left', borderBottom: `1px solid ${C.border}`, color: C.slate }}>Case ID</th>
              <th style={{ padding: '8px', textAlign: 'right', borderBottom: `1px solid ${C.border}`, color: C.slate }}>PO Qty</th>
              <th style={{ padding: '8px', textAlign: 'right', borderBottom: `1px solid ${C.border}`, color: C.slate }}>Netpr</th>
              <th style={{ padding: '8px', textAlign: 'right', borderBottom: `1px solid ${C.border}`, color: C.slate }}>Netwr</th>
              <th style={{ padding: '8px', textAlign: 'right', borderBottom: `1px solid ${C.border}`, color: C.slate }}>GR Qty</th>
              <th style={{ padding: '8px', textAlign: 'right', borderBottom: `1px solid ${C.border}`, color: C.slate }}>GR Amount</th>
              <th style={{ padding: '8px', textAlign: 'right', borderBottom: `1px solid ${C.border}`, color: C.slate }}>Invoice Qty</th>
              <th style={{ padding: '8px', textAlign: 'right', borderBottom: `1px solid ${C.border}`, color: C.slate }}>Invoice Amount</th>
            </tr>
          </thead>
          <tbody>
            {visibleData.map((row, i) => (
              <tr
                key={i}
                onClick={() => onSelect(row.case_id)}
                style={{
                  borderBottom: `1px solid ${C.border}`,
                  background: row.case_id === selectedId ? '#EFF6FF' : (i % 2 === 0 ? '#fff' : '#fafafa'),
                  cursor: 'pointer',
                  transition: 'background 0.2s',
                  borderLeft: row.case_id === selectedId ? `4px solid ${C.blue700}` : '4px solid transparent'
                }}
              >
                <td style={{ padding: '6px 8px', color: '#323130', fontWeight: 600, textAlign: 'left' }}>{row.case_id}</td>
                <td style={{ padding: '6px 8px', color: '#605E5C', textAlign: 'right' }}>{formatQty(row["PO Qty"])}</td>
                <td style={{ padding: '6px 8px', color: '#605E5C', textAlign: 'right' }}>{formatAmt(row["Netpr"])}</td>
                <td style={{ padding: '6px 8px', color: '#605E5C', textAlign: 'right' }}>{formatAmt(row["Netwr"])}</td>
                <td style={{ padding: '6px 8px', color: '#605E5C', textAlign: 'right' }}>{formatQty(row["GR Quantity"])}</td>
                <td style={{ padding: '6px 8px', color: '#605E5C', textAlign: 'right' }}>{formatAmt(row["GR Doc Amount"])}</td>
                <td style={{ padding: '6px 8px', color: '#605E5C', textAlign: 'right' }}>{formatQty(row["IR Quantity"])}</td>
                <td style={{ padding: '6px 8px', color: '#605E5C', textAlign: 'right' }}>{formatAmt(row["IR Doc Amount"])}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
});

/* ══════════════════════════════════════════
   CHARTS
══════════════════════════════════════════ */

const ActivityChart = React.memo(({ data, crossFilter, onSelect }) => {
  if (!Array.isArray(data) || !data.length) return <EmptyState condition={true} message="Data Not Uploaded / Available" />;
  const af = crossFilter?.type === 'activity' ? crossFilter.value : null;
  const rows = data.slice(0, 7);
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
      <div style={{ display: 'flex', gap: 14 }}>
        {[['#0078D4', 'Events'], ['#01a32a', 'Unique Cases']].map(([c, l]) => (
          <div key={l} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
            <div style={{ width: 10, height: 10, borderRadius: 2, background: c }} />
            <span style={{ fontSize: 10, color: C.slate }}>{l}</span>
          </div>
        ))}
      </div>
      <div style={{ width: '100%', height: 220 }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={rows} layout="vertical" margin={{ left: 40, right: 20, top: 4, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" horizontal={false} />
            <XAxis type="number" hide />
            <YAxis type="category" dataKey="activity" tick={{ fontSize: 11, fill: '#605E5C' }} width={135} interval={0} />
            <Tooltip cursor={{ fill: 'rgba(0,0,0,.03)' }} content={<CustomTooltip nameKey="activity" />} />

            <Bar dataKey="count" barSize={20}
              onClick={e => e?.activity && onSelect('activity', e.activity === af ? null : e.activity)}>
              {rows.map((e, i) => (
                <Cell key={i} cursor="pointer" fill={af === e?.activity ? '#CA5010' : '#0078D4'} opacity={af && af !== e?.activity ? 0.35 : 1} />
              ))}
            </Bar>

            <Bar dataKey="unique_cases" radius={[0, 3, 3, 0]} barSize={20}
              onClick={e => e?.activity && onSelect('activity', e.activity === af ? null : e.activity)}>
              {rows.map((e, i) => (
                <Cell key={i} cursor="pointer" fill={af === e?.activity ? '#999999' : '#268703'} opacity={af && af !== e?.activity ? 0.3 : 0.9} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
});

const MonthlyChart = React.memo(({ data, crossFilter, onSelect }) => {
  if (!Array.isArray(data) || !data.length) return <EmptyState condition={true} message="Data Not Uploaded / Available" />;
  return (
    <div style={{ width: '100%', height: 220 }}>
      <ResponsiveContainer width="100%" height="100%">
        <ComposedChart data={data} margin={{ left: 8, right: 8, top: 10, bottom: 40 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" />
          <XAxis dataKey="Month" tick={{ fontSize: 10, fill: '#605E5C' }} angle={-45} textAnchor="end" interval={0} />
          <YAxis tick={{ fontSize: 10, fill: '#605E5C' }} width={40} />
          <Tooltip content={<CustomTooltip nameKey="Month" labelOverride="cases" />} />
          <Bar dataKey="count" fill="transparent" cursor="pointer"
            onClick={e => e?.Month && onSelect('month', e.Month === crossFilter?.value ? null : e.Month)} />

          <Line
            type="monotone" dataKey="count" stroke="#0078D4" strokeWidth={2.5} cursor="pointer"
            dot={{ r: 3, fill: '#0078D4' }}
            activeDot={{
              r: 6, fill: '#CA5010', stroke: '#fff', strokeWidth: 2, cursor: 'pointer',
              onClick: (e, payload) => {
                if (payload && payload.payload && payload.payload.Month) {
                  onSelect('month', payload.payload.Month === crossFilter?.value ? null : payload.payload.Month);
                }
              }
            }}
            onClick={(e) => {
              if (e && e.activePayload && e.activePayload[0]) {
                const m = e.activePayload[0].payload.Month;
                onSelect('month', m === crossFilter?.value ? null : m);
              }
            }}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
});

const CompanyDonutChart = React.memo(({ data, crossFilter, onSelect }) => {
  if (!Array.isArray(data) || !data.length) return <EmptyState condition={true} message="Data Not Uploaded / Available" />;
  const af = crossFilter?.type === 'company' ? crossFilter.value : null;
  const total = data.reduce((s, d) => s + (d.count || 0), 0);

  const chartData = data.map((d, i) => ({
    ...d,
    fill: ACCENT[i % ACCENT.length]
  }));

  const renderLabel = ({ cx, cy, midAngle, innerRadius, outerRadius, percent }) => {
    if (percent < 0.05) return null;
    const R = Math.PI / 180, r = innerRadius + (outerRadius - innerRadius) * .55;
    const x = cx + r * Math.cos(-midAngle * R), y = cy + r * Math.sin(-midAngle * R);
    return (
      <text x={x} y={y} fill="#fff" textAnchor="middle" dominantBaseline="central"
        style={{ fontSize: 10, fontWeight: 700, pointerEvents: 'none' }}>
        {`${(percent * 100).toFixed(0)}%`}
      </text>
    );
  };
  return (
    <div style={{ width: '100%', height: 260, position: 'relative' }}>
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Pie data={chartData} dataKey="count" nameKey="company"
            cx="40%" cy="50%" innerRadius={60} outerRadius={95}
            paddingAngle={2} labelLine={false} label={renderLabel}
            onClick={e => e?.company && onSelect('company', e.company === af ? null : e.company)}>
            {chartData.map((entry, i) => (
              <Cell key={i} fill={entry.fill} cursor="pointer"
                opacity={af && af !== entry?.company ? 0.2 : 1}
                stroke={af === entry?.company ? '#323130' : 'none'}
                strokeWidth={af === entry?.company ? 2 : 0} />
            ))}
          </Pie>
          <text x="37.5%" y="46%" textAnchor="middle" dominantBaseline="middle" style={{ fontSize: 18, fontWeight: 800, fill: '#323130' }}>
            {Number(af ? (chartData.find(d => d.company === af)?.count || 0) : total).toLocaleString()}
          </text>
          <text x="37.5%" y="53%" textAnchor="middle" dominantBaseline="middle" style={{ fontSize: 10, fill: '#8A8886', fontWeight: 600 }}>
            {af || 'Total'}
          </text>
          <Tooltip formatter={v => [Number(v).toLocaleString(), 'Cases']} contentStyle={{ background: '#fff', border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 12 }} />
          <Legend
            layout="vertical"
            verticalAlign="middle"
            align="right"
            iconType="circle"
            wrapperStyle={{ fontSize: '11px', cursor: 'pointer', right: 10 }}
            onClick={(entry) => {
              if (entry && entry.value) {
                onSelect('company', entry.value === af ? null : entry.value);
              }
            }}
          />
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
});

const StatusDonutChart = React.memo(({ data, crossFilter, onSelect }) => {
  if (!Array.isArray(data) || !data.length) return <EmptyState condition={true} message="Data Not Uploaded / Available" />;
  const af = crossFilter?.type === 'status' ? crossFilter.value : null;
  const total = data.reduce((s, d) => s + (d.count || 0), 0);

  const chartData = data.map((d) => ({
    ...d,
    fill: d.status === 'Happy Path' ? '#107C10' : '#D13438'
  }));

  const renderLabel = ({ cx, cy, midAngle, innerRadius, outerRadius, percent }) => {
    if (percent < 0.05) return null;
    const R = Math.PI / 180, r = innerRadius + (outerRadius - innerRadius) * .55;
    const x = cx + r * Math.cos(-midAngle * R), y = cy + r * Math.sin(-midAngle * R);
    return (
      <text x={x} y={y} fill="#fff" textAnchor="middle" dominantBaseline="central"
        style={{ fontSize: 10, fontWeight: 700, pointerEvents: 'none' }}>
        {`${(percent * 100).toFixed(0)}%`}
      </text>
    );
  };
  return (
    <div style={{ width: '100%', height: 260, position: 'relative' }}>
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Pie data={chartData} dataKey="count" nameKey="status"
            cx="40%" cy="50%" innerRadius={60} outerRadius={95}
            paddingAngle={2} labelLine={false} label={renderLabel}
            onClick={e => e?.status && onSelect('status', e.status === af ? null : e.status)}>
            {chartData.map((entry, i) => (
              <Cell key={i} fill={entry.fill} cursor="pointer"
                opacity={af && af !== entry?.status ? 0.2 : 1}
                stroke={af === entry?.status ? '#323130' : 'none'}
                strokeWidth={af === entry?.status ? 2 : 0} />
            ))}
          </Pie>
          <text x="33%" y="46%" textAnchor="middle" dominantBaseline="middle" style={{ fontSize: 18, fontWeight: 800, fill: '#323130' }}>
            {Number(af ? (chartData.find(d => d.status === af)?.count || 0) : total).toLocaleString()}
          </text>
          <text x="33%" y="53%" textAnchor="middle" dominantBaseline="middle" style={{ fontSize: 10, fill: '#8A8886', fontWeight: 600 }}>
            {af || 'Total Cases'}
          </text>
          <Tooltip formatter={v => [Number(v).toLocaleString(), 'Cases']} contentStyle={{ background: '#fff', border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 12 }} />
          <Legend
            layout="vertical"
            verticalAlign="middle"
            align="right"
            iconType="circle"
            wrapperStyle={{ fontSize: '11px', cursor: 'pointer', right: 10 }}
            onClick={(entry) => {
              if (entry && entry.value) {
                onSelect('status', entry.value === af ? null : entry.value);
              }
            }}
          />
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
});

const ScrollableHBarChart = React.memo(({ data, dataKey, labelKey, crossFilter, crossKey, onSelect, color }) => {
  if (!Array.isArray(data) || !data.length) return <EmptyState condition={true} message="Data Not Uploaded / Available" />;
  const af = crossFilter?.type === crossKey ? crossFilter.value : null;
  const rowH = 30;
  const chartH = Math.max(220, data.length * rowH);
  return (
    <div style={{ width: '100%', height: 220, overflowY: 'auto', paddingRight: 8 }}>
      <div style={{ height: chartH }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} layout="vertical" margin={{ left: 10, right: 20, top: 4, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" horizontal={false} />
            <XAxis type="number" hide />
            <YAxis type="category" dataKey={labelKey} tick={{ fontSize: 10, fill: '#605E5C' }} width={120} interval={0} />
            <Tooltip cursor={{ fill: 'rgba(0,0,0,.04)' }} content={<CustomTooltip nameKey={labelKey} labelOverride="cases" />} />
            <Bar dataKey={dataKey} radius={[0, 3, 3, 0]} barSize={20}
              onClick={e => e && e[labelKey] && onSelect(crossKey, e[labelKey] === af ? null : e[labelKey])}>
              {data.map((entry, i) => (
                <Cell key={i} cursor="pointer" fill={color || '#5C2D91'}
                  opacity={af && af !== entry?.[labelKey] ? 0.25 : 1} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
});

const ScrollableVBarChart = React.memo(({ data, crossFilter, onSelect }) => {
  if (!Array.isArray(data) || !data.length) return <EmptyState condition={true} message="Data Not Uploaded / Available" />;
  const af = crossFilter?.type === 'bsart' ? crossFilter.value : null;
  const colW = 50;
  const chartW = Math.max('100%', data.length * colW);
  return (
    <div style={{ width: '100%', height: 259, overflowX: 'auto', overflowY: 'hidden' }}>
      <div style={{ width: chartW, height: '100%' }}>
        <ResponsiveContainer width="100%" height="110%">
          <BarChart data={data} margin={{ left: 8, right: 16, top: 10, bottom: 40 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" vertical={false} />
            <XAxis dataKey="bsart" tick={{ fontSize: 11, fill: '#605E5C' }} angle={-40} textAnchor="end" interval={0} />
            <YAxis tick={{ fontSize: 10, fill: '#605E5C' }} width={40} />
            <Tooltip cursor={{ fill: 'rgba(0,0,0,.04)' }} content={<CustomTooltip nameKey="bsart" labelOverride="cases" />} />
            <Bar dataKey="count" radius={[4, 4, 0, 0]}
              onClick={e => e && e.bsart && onSelect('bsart', e.bsart === af ? null : e.bsart)}>
              {data.map((entry, i) => (
                <Cell key={i} cursor="pointer" fill={ACCENT[i % ACCENT.length]} opacity={af && af !== entry?.bsart ? 0.25 : 1} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
});

const LeadTimeChart = React.memo(({ data, crossFilter, onSelect }) => {
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
          <Bar dataKey="count" radius={[2, 2, 0, 0]}
            onClick={e => e?.label && onSelect('lead_time', e.label === af ? null : e.label)}>
            {data.map((entry, i) => (
              <Cell key={i} cursor="pointer" fill={af === entry.label ? '#CA5010' : '#038387'}
                opacity={af && af !== entry.label ? 0.35 : 1} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
});

const ErnamChart = React.memo(({ data, crossFilter, onSelect }) => {
  if (!Array.isArray(data) || !data.length) return <EmptyState condition={true} message="Data Not Uploaded / Available" />;

  const af = crossFilter?.type === 'ernam' ? crossFilter.value : null;
  const sorted = [...data].sort((a, b) => b.count - a.count);
  const rowH = 30;
  const chartH = Math.max(220, sorted.length * rowH);

  return (
    <div style={{ width: '100%', height: 220, overflowY: 'auto', paddingRight: 8 }}>
      <div style={{ height: chartH }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={sorted} layout="vertical" margin={{ left: 10, right: 20, top: 4, bottom: 4 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" horizontal={false} />
            <XAxis type="number" hide />
            <YAxis type="category" dataKey="ernam" tick={{ fontSize: 10, fill: '#605E5C' }} width={90} interval={0} />
            <Tooltip cursor={{ fill: 'rgba(0,0,0,.04)' }} content={<CustomTooltip nameKey="ernam" labelOverride="cases" />} />
            <Bar dataKey="count" radius={[0, 3, 3, 0]} barSize={20}
              onClick={e => e?.ernam && onSelect('ernam', e.ernam === af ? null : e.ernam)}>
              {sorted.map((entry, i) => (
                <Cell key={i} cursor="pointer" fill="#2254b1" opacity={af && af !== entry?.ernam ? 0.25 : 1} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
});

/* ─── PO REVERSALS BY PURCHASING GROUP — GRADIENT VBAR SCROLLABLE ─ */
const RevByPurchGroupVBarChart = React.memo(({ data, crossFilter, onSelect }) => {
  if (!Array.isArray(data) || !data.length) return <Empty />;
  const af = crossFilter?.type === 'purch_group' ? crossFilter.value : null;
  const colW = 72;
  const chartW = Math.max(600, data.length * colW);
  return (
    <div style={{ width: '100%', height: 260, overflowX: 'auto', overflowY: 'hidden' }}>
      <div style={{ width: chartW, height: '100%' }}>
        <ResponsiveContainer width="100%" height="120%">
          <BarChart data={data} margin={{ left: 8, right: 16, top: 16, bottom: 52 }}>
            <defs>
              <linearGradient id="revPGGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#5C2D91" stopOpacity={1} />
                <stop offset="100%" stopColor="#8B5CF6" stopOpacity={0.7} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" vertical={false} />
            <XAxis
              dataKey="purch_group"
              tick={{ fontSize: 11, fill: '#605E5C' }}
              angle={-38} textAnchor="end" interval={0}
            />
            <YAxis tick={{ fontSize: 10, fill: '#605E5C' }} width={44} />
            <Tooltip
              cursor={{ fill: 'rgba(92,45,145,.06)' }}
              content={<CustomTooltip nameKey="purch_group" labelOverride="cases" />}
            />
            <Bar dataKey="count" radius={[5, 5, 0, 0]}
              onClick={e => e?.purch_group && onSelect('purch_group', e.purch_group === af ? null : e.purch_group)}>
              {data.map((entry, i) => (
                <Cell
                  key={i}
                  cursor="pointer"
                  fill={af === entry.purch_group ? '#CA5010' : 'url(#revPGGrad)'}
                  opacity={af && af !== entry.purch_group ? 0.28 : 1}
                  stroke={af === entry.purch_group ? '#CA5010' : 'none'}
                  strokeWidth={af === entry.purch_group ? 1.5 : 0}
                />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
});

/* ─── SOD VIOLATIONS CHART ─────────────────────────────────────── */
const SodViolationsChart = React.memo(({ data, crossFilter, onSelect }) => {
  if (!Array.isArray(data) || !data.length) return <Empty />;
  const af = crossFilter?.type === 'sod' ? crossFilter.value : null;
  const colW = 160;
  const chartW = Math.max(500, data.length * colW);
  return (
    <div style={{ width: '100%', height: 205, overflowX: 'auto', overflowY: 'hidden' }}>
      <div style={{ width: chartW, height: '100%' }}>
        <ResponsiveContainer width="120%" height="100%">
          <BarChart data={data} margin={{ left: 8, right: 24, top: 16, bottom: 56 }}>
            <defs>
              <linearGradient id="sodGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#D13438" stopOpacity={0.9} />
                <stop offset="100%" stopColor="#CA5010" stopOpacity={0.65} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" vertical={false} />
            <XAxis dataKey="violation" tick={{ fontSize: 11, fill: '#605E5C' }} angle={-28} textAnchor="end" interval={0} />
            <YAxis tick={{ fontSize: 10, fill: '#605E5C' }} width={44} />
            <Tooltip cursor={{ fill: 'rgba(209,52,56,.05)' }} content={<CustomTooltip nameKey="violation" labelOverride="cases" />} />
            <Bar dataKey="count" radius={[5, 5, 0, 0]}
              onClick={e => e?.violation && onSelect('sod', e.violation === af ? null : e.violation)}>
              {data.map((entry, i) => (
                <Cell key={i} cursor="pointer"
                  fill={af === entry.violation ? '#CA5010' : 'url(#sodGrad)'}
                  opacity={af && af !== entry.violation ? 0.28 : 1}
                  stroke={af === entry.violation ? '#CA5010' : 'none'}
                  strokeWidth={af === entry.violation ? 1.5 : 0}
                />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
});

/* ─── BOTTLENECK CHART ─────────────────────────────────────────── */
const BottleneckTooltip = ({ active, payload }) => {
  if (!active || !payload?.length) return null;
  const d = payload[0].payload;
  return (
    <div style={{ background: 'rgba(255,255,255,.98)', border: `1px solid #E1DFDD`, borderRadius: 6, padding: '8px 14px', fontSize: 12, color: '#323130' }}>
      <div style={{ fontWeight: 700, color: '#0078D4', marginBottom: 4 }}>{d.step}</div>
      <div style={{ color: '#605E5C' }}>Avg Days: <strong style={{ color: '#323130' }}>{d.avg_days}</strong></div>
      <div style={{ color: '#605E5C' }}>Median Days: <strong style={{ color: '#038387' }}>{d.median_days}</strong></div>
      <div style={{ color: '#605E5C' }}>Cases: <strong>{Number(d.count).toLocaleString()}</strong></div>
    </div>
  );
};

const BottleneckChart = React.memo(({ data }) => {
  if (!Array.isArray(data) || !data.length) return <Empty />;
  return (
    <div style={{ width: '100%', height: 220 }}>
      <ResponsiveContainer width="100%" height="110%">
        <ComposedChart data={data} margin={{ left: 8, right: 20, top: 8, bottom: 60 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" vertical={false} />
          <XAxis dataKey="step" tick={{ fontSize: 10, fill: '#605E5C' }} angle={-30} textAnchor="end" interval={0} />
          <YAxis tick={{ fontSize: 10, fill: '#605E5C' }} width={45} label={{ value: 'Days', angle: -90, position: 'insideLeft', offset: 5, style: { fontSize: 10, fill: '#605E5C' } }} />
          <Tooltip content={<BottleneckTooltip />} />
          <Bar dataKey="avg_days" name="Avg Days" radius={[4, 4, 0, 0]} fill="#4F6BED" />
          <Line type="monotone" dataKey="median_days" name="Median Days" stroke="#CA5010" strokeWidth={2} dot={{ fill: '#CA5010', r: 4 }} />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
});

/* ─── PURCHASING GROUP WORKLOAD — GRADIENT VERTICAL BAR ────────── */
const PurchGroupWorkloadChart = React.memo(({ data, crossFilter, onSelect }) => {
  if (!Array.isArray(data) || !data.length) return <Empty />;
  const af = crossFilter?.type === 'purch_group' ? crossFilter.value : null;
  const colW = 64;
  const chartW = Math.max(500, data.length * colW);

  const gradId = 'pgGrad';

  return (
    <div style={{ width: '100%', height: 260, overflowX: 'auto', overflowY: 'hidden' }}>
      <div style={{ width: chartW, height: '100%' }}>
        <ResponsiveContainer width="100%" height="120%">
          <BarChart data={data} margin={{ left: 8, right: 16, top: 16, bottom: 48 }}>
            <defs>
              <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#0078D4" stopOpacity={1} />
                <stop offset="100%" stopColor="#00B7C3" stopOpacity={0.75} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" vertical={false} />
            <XAxis
              dataKey="purch_group"
              tick={{ fontSize: 11, fill: '#605E5C' }}
              angle={-38} textAnchor="end" interval={0}
            />
            <YAxis tick={{ fontSize: 10, fill: '#605E5C' }} width={44} />
            <Tooltip
              cursor={{ fill: 'rgba(0,120,212,.06)' }}
              content={<CustomTooltip nameKey="purch_group" labelOverride="cases" />}
            />
            <Bar dataKey="count" radius={[5, 5, 0, 0]}
              onClick={e => e?.purch_group && onSelect('purch_group', e.purch_group === af ? null : e.purch_group)}>
              {data.map((entry, i) => (
                <Cell
                  key={i}
                  cursor="pointer"
                  fill={af === entry.purch_group ? '#CA5010' : `url(#${gradId})`}
                  opacity={af && af !== entry.purch_group ? 0.3 : 1}
                  stroke={af === entry.purch_group ? '#CA5010' : 'none'}
                  strokeWidth={af === entry.purch_group ? 1.5 : 0}
                />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
});

/* ─── VENDOR AVG PO-TO-INVOICE DAYS CHART ─────────────────────── */
const VendorAvgDaysTooltip = ({ active, payload }) => {
  if (!active || !payload?.length) return null;
  const d = payload[0].payload;
  return (
    <div style={{ background: 'rgba(255,255,255,.98)', border: `1px solid #E1DFDD`, borderRadius: 6, padding: '8px 14px', fontSize: 12, color: '#323130', maxWidth: 240 }}>
      <div style={{ fontWeight: 700, color: '#038387', marginBottom: 4, wordBreak: 'break-word' }}>{d.vendor}</div>
      <div style={{ color: '#605E5C' }}>Avg Days (PO→GR): <strong style={{ color: '#323130' }}>{d.avg_days}d</strong></div>
      <div style={{ color: '#605E5C', marginTop: 2 }}>Cases: <strong>{Number(d.case_count).toLocaleString()}</strong></div>
    </div>
  );
};

const VendorAvgDaysChart = React.memo(({ data, crossFilter, onSelect }) => {
  if (!Array.isArray(data) || !data.length) return <Empty />;

  const af = crossFilter?.type === 'vendor' ? crossFilter.value : null;
  const sorted = [...data].sort((a, b) => b.avg_days - a.avg_days);

  const colW = 72;
  const chartW = Math.max(500, sorted.length * colW);

  return (
    <div style={{ width: '100%', height: 275, overflowX: 'auto', overflowY: 'hidden', minWidth: 0, maxWidth: '100%' }}>
      <div style={{ width: chartW, height: '100%', minWidth: chartW }}>
        <ResponsiveContainer width="100%" height="110%">
          <BarChart data={sorted} margin={{ left: 8, right: 16, top: 16, bottom: 52 }}>
            <defs>
              <linearGradient id="vendAvgGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#038387" stopOpacity={1} />
                <stop offset="100%" stopColor="#00B7C3" stopOpacity={0.7} />
              </linearGradient>
              <linearGradient id="vendAvgGradActive" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#CA5010" stopOpacity={1} />
                <stop offset="100%" stopColor="#F59E0B" stopOpacity={0.8} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" vertical={false} />
            <XAxis
              dataKey="vendor"
              tick={{ fontSize: 10, fill: '#605E5C' }}
              angle={-38} textAnchor="end" interval={0}
              tickFormatter={v => v && v.length > 14 ? v.slice(0, 13) + '…' : v}
            />
            <YAxis
              tick={{ fontSize: 10, fill: '#605E5C' }}
              width={44}
              label={{ value: 'Avg Days', angle: -90, position: 'insideLeft', offset: 8, style: { fontSize: 10, fill: '#8A8886' } }}
            />
            <Tooltip content={<VendorAvgDaysTooltip />} cursor={{ fill: 'rgba(3,131,135,.08)' }} />
            <Bar
              dataKey="avg_days"
              radius={[5, 5, 0, 0]}
              onClick={e => e?.vendor && onSelect('vendor', e.vendor === af ? null : e.vendor)}
            >
              {sorted.map((entry, i) => (
                <Cell
                  key={i}
                  cursor="pointer"
                  fill={af === entry.vendor ? 'url(#vendAvgGradActive)' : 'url(#vendAvgGrad)'}
                  opacity={af && af !== entry.vendor ? 0.28 : 1}
                  stroke={af === entry.vendor ? '#CA5010' : 'none'}
                  strokeWidth={af === entry.vendor ? 1.5 : 0}
                />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
});

/* ─── UPLOAD BANNER ────────────────────────────────────────────── */
/* ─── P2P INTRO SCREEN ──────────────────────────────────────────────────── */
/* ── FAQ Accordion Item ── */
const FaqItem = ({ q, a, bullets, accentColor }) => {
  const [open, setOpen] = useState(false);
  const accent = accentColor || '#0078D4';
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
        <div style={{ paddingBottom: 16, fontSize: 13, color: '#475569', lineHeight: 1.75 }}>
          {a && <p style={{ margin: '0 0 8px' }}>{a}</p>}
          {bullets && bullets.length > 0 && (
            <ul style={{ margin: 0, paddingLeft: 20, display: 'flex', flexDirection: 'column', gap: 5 }}>
              {bullets.map((b, i) => (
                <li key={i} style={{ color: '#334155', lineHeight: 1.6 }}>
                  {typeof b === 'object' && b.bold
                    ? <><strong style={{ color: '#1e293b' }}>{b.bold}</strong>{b.rest}</>
                    : b
                  }
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  );
};

const P2P_FAQS = [
  {
    q: 'What is the Procure-to-Pay (P2P) process?',
    a: 'The P2P process is the end-to-end workflow for acquiring goods and services, from identifying a need to making the final payment. It integrates procurement and accounts payable to improve efficiency, control spend, ensure compliance, and reduce errors. Key stages include:',
    bullets: [
      { bold: 'Purchase Requisition:', rest: ' Identifying needs and creating an internal purchase request' },
      { bold: 'Purchase Order:', rest: ' Sourcing suppliers and creating/approving a PO' },
      { bold: 'Goods Receipt:', rest: ' Receiving and confirming delivery of goods or services' },
      { bold: 'Invoice Validation:', rest: ' Receiving and verifying the vendor invoice' },
      { bold: 'Payment:', rest: ' Paying the supplier and recording the transaction' },
    ],
  },
  {
    q: 'What is Process Mining, and how does it apply to P2P?',
    a: 'Process Mining analyzes real event log data from any ERP systems (SAP, Oracle, D365) to reconstruct and visualize how the P2P process actually runs — not just how it was designed. In P2P it creates a "digital twin" of procurement activities, revealing:',
    bullets: [
      'The true process flow with all variants and deviations',
      'Bottlenecks causing delays in approvals or goods receipt',
      'Maverick buying and compliance violations',
      'Missed early-payment discounts and value leaks',
    ],
  },
  {
    q: 'Why use Process Mining specifically for P2P?',
    a: 'P2P processes are often complex, cross-departmental, and span multiple systems. Process Mining delivers:',
    bullets: [
      { bold: 'Objective visibility:', rest: ' See actual execution, not just designed or reported flows' },
      { bold: 'Compliance detection:', rest: ' Identify purchases without POs or approval bypasses' },
      { bold: 'Value leak quantification:', rest: ' Measure missed discounts and rework costs' },
      { bold: 'Benchmarking:', rest: ' Compare performance across regions, buyers, or suppliers' },
      { bold: 'Automation targeting:', rest: ' Prioritize RPA or redesign based on data' },
    ],
  },
  {
    q: 'What makes ALTeX Hub process mining different?',
    a: 'The process mining done under ALTeX Hub is different from traditional process mining tools because:',
    bullets: [
      'Runs in parallel to the CompliBear ACM engine',
      'It uses machine learning to analyze and interpret process data',
      'CoSaas: Each run is customised to the business, rather than relying on the data or standard happy path guidelines for the processes',
      'Interconnected to other processes and systems to bring all the touchpoints as a single source of truth.',
    ],
  },
  {
    q: 'What key metrics (KPIs) does Process Mining help track in P2P?',
    a: 'Common KPIs tracked include:',
    bullets: [
      { bold: 'Purchase/PO Cycle Time:', rest: ' From requisition creation to PO approval' },
      { bold: 'PO Compliance Rate:', rest: ' % of spend covered by approved Purchase Orders' },
      { bold: 'Invoice Processing Time:', rest: ' Days from invoice receipt to payment' },
      { bold: 'Three-Way Match Rate:', rest: ' PO vs Goods Receipt vs Invoice alignment' },
      { bold: 'Supplier Lead Time Variability:', rest: ' Consistency of vendor delivery performance' },
      { bold: 'Maverick Spend %:', rest: ' Purchases made outside approved procurement channels' },
      { bold: 'Spaghetti Processes:', rest: ' High number of process variants across the normal purchasing channels' },
    ],
  },
  {
    q: 'What are the top use cases for Process Mining in P2P?',
    a: 'Key use cases include:',
    bullets: [
      'Reducing cycle times and bottlenecks in approvals and invoice matching',
      'Detecting fraud, compliance risks, or duplicate payments',
      'Benchmarking performance across business units or suppliers',
      'Identifying segregation-of-duties (SoD) violations',
      'Prioritizing automation candidates for RPA or AI-driven workflows',
    ],
  },
  {
    q: 'What data is required for Process Mining in P2P?',
    a: 'Event logs need at minimum:',
    bullets: [
      { bold: 'Case ID:', rest: ' A unique identifier such as PO number or requisition ID' },
      { bold: 'Activity + Timestamp:', rest: ' e.g., "Create PO", "Goods Receipt", "Invoice Posted"' },
      { bold: 'Optional attributes:', rest: ' Requester, approver, supplier, value, material group, department' },
      { bold: 'Data sources:', rest: ' SAP ECC/S/4HANA (EKKO, EKPO, EBAN, EKBE), Oracle ERP, procurement and AP tools' },
    ],
  },
  {
    q: 'What challenges come with applying Process Mining to P2P?',
    a: 'Common challenges include:',
    bullets: [
      'Data quality and extraction issues — incomplete logs or data spread across multiple systems',
      'Change management — teams resisting transparency into actual process performance',
      'Scope creep — starting too broad rather than focusing on targeted use cases',
      'Integration with action tools (RPA, workflow engines) for remediation',
      'Privacy and compliance concerns around sensitive procurement data',
    ],
  },
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

const P2PIntroScreen = ({ onGoTableBuild, onGoCsvUpload, currentUser, introStep, setIntroStep }) => {
  const [hoveredSide, setHoveredSide] = useState(null);
  const [showFaqModal, setShowFaqModal] = useState(false);

  const steps = [
    'Purchase Requisition (PR) Creation & Request',
    'PR Approval & Budget Verification',
    'Purchase Order (PO) Creation & Vendor Sync',
    'PO Release & Vendor Acknowledgement',
    'Goods Receipt (GR) Posting & Delivery',
    'Invoice Receipt (IR) & Verification',
    'Three-way Match (PO vs GR vs IR) Check',
    'Payment Clearing & Vendor Settlement',
  ];
  const kpis = [
    'PR-to-PO conversion time', 'PO-to-GR lead time', 'GR-to-IR matching rate',
    'Invoice processing cycle time', 'Three-way match exception rate', 'Vendor on-time delivery %',
  ];

  const IconWrapper = ({ children, color }) => (
    <div style={{
      width: 20, height: 20, display: 'flex', alignItems: 'center', justifyContent: 'center',
      color: color, flexShrink: 0
    }}>
      {children}
    </div>
  );

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
        <path d="M4 2v20l2-1 2 1 2-1 2 1 2-1 2 1 2-1 2 1V2l-2 1-2-1-2 1-2-1-2 1-2-1-2 1-2-1z" /><line x1="16" y1="8" x2="8" y2="8" /><line x1="16" y1="12" x2="8" y2="12" /><line x1="16" y1="16" x2="8" y2="16" />
      </svg>
    ),
    creditCard: (
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
        <rect x="1" y="4" width="22" height="16" rx="2" ry="2" /><line x1="1" y1="10" x2="23" y2="10" />
      </svg>
    )
  };

  const shortSteps = [
    { icon: icons.file, text: 'PR' },
    { icon: icons.check, text: 'PR Release' },
    { icon: icons.check, text: 'PO Created' },
    { icon: icons.check, text: 'PO Release' },
    { icon: icons.check, text: 'GR' },
    { icon: icons.receipt, text: 'Invoice' },
    { icon: icons.creditCard, text: 'Payment' },
  ];

  const KeyMetricsAnimation = ({ metrics }) => {
    const colors = ['#0078D4', '#005A9E', '#00B7C3', '#008272', '#107C10', '#038387'];
    const subtitles = [
      'Days to convert purchase requisition to purchase order',
      'Days from order placement to goods receipt',
      'Percent of receipts matching invoices',
      'Time taken to verify and clear vendor invoices',
      'Percent of invoices with price/quantity discrepancies',
      'Percent of deliveries received on or before deadline'
    ];

    const icons = [
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10" /><polyline points="12 6 12 12 16 14" /></svg>,
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14" /><polyline points="22 4 12 14.01 9 11.01" /></svg>,
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><polyline points="23 6 13.5 15.5 8.5 10.5 1 18" /><polyline points="17 6 23 6 23 12" /></svg>,
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><rect x="3" y="4" width="18" height="18" rx="2" ry="2" /><line x1="16" y1="2" x2="16" y2="6" /><line x1="8" y1="2" x2="8" y2="6" /><line x1="3" y1="10" x2="21" y2="10" /></svg>,
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z" /><line x1="12" y1="9" x2="12" y2="13" /><line x1="12" y1="17" x2="12.01" y2="17" /></svg>,
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><rect x="1" y="3" width="15" height="13" /><polygon points="16 8 20 8 23 11 23 16 16 16 16 8" /><circle cx="5.5" cy="18.5" r="2.5" /><circle cx="18.5" cy="18.5" r="2.5" /></svg>
    ];

    return (
      <div style={{ display: 'flex', flexDirection: 'row', alignItems: 'center', gap: '12px', width: '100%', height: '302px' }}>
        <KpiWheel colors={colors} label="P2P" height={302} width={140} />
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
    const colors = ['#0078D4', '#00B7C3', '#1057caf6', '#0568c3ff', '#492f9cff', '#082e56ff'];
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
            <p style={{ margin: '4px 0 0', fontSize: '14px', color: '#64748b' }}>Learn more about P2P Process Mining</p>
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
            {P2P_FAQS.map((faq, i) => (
              <FaqItem key={i} q={faq.q} a={faq.a} bullets={faq.bullets} accentColor="#0078D4" />
            ))}
          </div>
        </div>
      </motion.div>
    </motion.div>
  );
  if (introStep === 'overview') {
    return (
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', overflowY: 'auto', position: 'relative' }}>
        <AnimatePresence>
          {showFaqModal && <FaqModal />}
        </AnimatePresence>

        {/* Continuous Process Ribbon */}
        <div className="process-ribbon-container">
          <div className="process-ribbon-content">
            {/* Render twice for seamless looping */}
            {[...shortSteps, ...shortSteps, ...shortSteps, ...shortSteps].map((s, i) => {
              const isFirst = i % shortSteps.length === 0;
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
                  {i < shortSteps.length * 4 - 1 && <div className="process-ribbon-arrow">→</div>}
                </React.Fragment>
              );
            })}
          </div>
        </div>

        <div style={{ maxWidth: 1100, width: '100%', display: 'flex', flexDirection: 'column', gap: 16, padding: '20px 24px 30px' }}>

          {/* Header */}
          <div style={{ borderBottom: '2px solid #E2E8F0', paddingBottom: 12, display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
            <div style={{ flex: 1 }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: '#0078D4', textTransform: 'uppercase', letterSpacing: 1, marginBottom: 4 }}>Process Overview</div>
              <h1 style={{ margin: '0 0 6px', fontSize: 22, fontWeight: 700, color: '#1e293b' }}>Procure-to-Pay (P2P)</h1>
              <p style={{ margin: 0, fontSize: 13, color: '#475569', lineHeight: 1.6, maxWidth: 900 }}>
                The Procure-to-Pay process spans the full purchasing lifecycle — from identifying a business need
                through raising a requisition, placing a purchase order with a vendor, receiving goods, and finally
                clearing the vendor invoice. Process mining on P2P data helps uncover delays, duplicate payments,
                three-way match failures, and segregation-of-duties violations.
              </p>
            </div>
            <button
              onClick={() => setShowFaqModal(true)}
              style={{
                background: 'linear-gradient(135deg, #0078D4 0%, #00B7C3 100%)',
                color: '#fff', border: 'none', padding: '10px 20px', borderRadius: '12px',
                fontSize: '13px', fontWeight: 700, cursor: 'pointer', display: 'flex',
                alignItems: 'center', gap: '8px', boxShadow: '0 4px 12px rgba(0, 120, 212, 0.2)',
                transition: 'all 0.2s', flexShrink: 0, marginTop: '10px'
              }}
              onMouseOver={e => e.currentTarget.style.transform = 'translateY(-2px)'}
              onMouseOut={e => e.currentTarget.style.transform = 'translateY(0)'}
            >
              <span>Read FAQ</span>
              <span style={{ fontSize: '16px' }}>💬</span>
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

          {/* Continue button — bottom right */}
          <div style={{ display: 'flex', justifyContent: 'flex-end', paddingTop: 4 }}>
            <button
              onClick={() => setIntroStep('choose')}
              style={{
                background: '#0078D4', color: '#fff', border: 'none',
                padding: '10px 28px', borderRadius: 8, fontSize: 13, fontWeight: 700,
                cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 8,
                boxShadow: '0 4px 12px rgba(0,120,212,0.25)', transition: 'all 0.2s',
              }}
              onMouseOver={e => { e.currentTarget.style.background = '#005A9E'; e.currentTarget.style.boxShadow = '0 6px 16px rgba(0,120,212,0.35)'; }}
              onMouseOut={e => { e.currentTarget.style.background = '#0078D4'; e.currentTarget.style.boxShadow = '0 4px 12px rgba(0,120,212,0.25)'; }}>
              Continue
              <span style={{ fontSize: 14 }}>→</span>
            </button>
          </div>
        </div>
      </div>
    );
  }

  /* ── Choose your path ── */
  return (
    <div style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', padding: '2rem', overflowY: 'auto', position: 'relative' }}>
      {/* Snapshot Button */}
      <button
        onClick={() => window.open('/snapshot p2p.pdf', '_blank')}
        style={{
          position: 'absolute',
          top: '24px',
          right: '24px',
          background: '#0057B7', color: '#fff', border: 'none',
          padding: '9px 18px', borderRadius: '8px', fontSize: '12px',
          fontWeight: 700, cursor: 'pointer',
          display: 'flex', alignItems: 'center', gap: '6px',
          boxShadow: '0 4px 12px rgba(0,87,183,0.3)',
          transition: 'all 0.2s',
          zIndex: 10
        }}
        onMouseOver={e => { e.currentTarget.style.filter = 'brightness(1.1)'; e.currentTarget.style.transform = 'translateY(-1px)'; }}
        onMouseOut={e => { e.currentTarget.style.filter = 'none'; e.currentTarget.style.transform = 'none'; }}
      >
        📸 Snapshot
      </button>

      <div style={{ maxWidth: 880, width: '100%', display: 'flex', flexDirection: 'column', gap: 24 }}>


        <div style={{ textAlign: 'center' }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: 0.8, marginBottom: 8 }}>Get Started</div>
          <h2 style={{ margin: '0 0 6px', fontSize: 20, fontWeight: 700, color: '#1e293b' }}>Choose how to load your data</h2>
          <p style={{ margin: 0, fontSize: 13, color: '#64748b' }}>Select the method that matches your data format</p>
        </div>

        <div style={{ display: 'flex', width: '100%', height: 320, gap: 16 }}>
          {/* Build Event Log panel */}
          <motion.div
            layout
            style={{
              flex: hoveredSide === 'build' ? 1.7 : (hoveredSide === 'csv' ? 0.6 : 1),
              background: '#fff', border: '2px solid #E2E8F0', borderRadius: 16, padding: '28px 24px',
              display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 14, textAlign: 'center',
              boxShadow: '0 4px 12px rgba(0,0,0,0.03)', cursor: 'pointer', overflow: 'hidden'
            }}
            onClick={onGoTableBuild}
            onMouseEnter={() => setHoveredSide('build')}
            onMouseLeave={() => setHoveredSide(null)}
            animate={{ borderColor: hoveredSide === 'build' ? '#0078D4' : '#E2E8F0' }}
          >
            <motion.div layout style={{
              width: 60, height: 60, borderRadius: 14, background: '#eff6ff',
              display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0
            }}>
              <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="#0078D4" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round">
                <rect x="2" y="3" width="6" height="6" rx="1" />
                <rect x="16" y="3" width="6" height="6" rx="1" />
                <rect x="16" y="15" width="6" height="6" rx="1" />
                <rect x="2" y="15" width="6" height="6" rx="1" />
                <path d="M8 6h8M19 9v6M16 18H8M5 15V9" />
              </svg>
            </motion.div>
            <motion.div layout style={{ flex: 1 }}>
              <motion.div layout style={{ fontSize: 18, fontWeight: 700, color: '#1e293b', marginBottom: 8, whiteSpace: hoveredSide === 'csv' ? 'nowrap' : 'normal' }}>Build Event Log</motion.div>
              <AnimatePresence>
                {hoveredSide !== 'csv' && (
                  <motion.div
                    initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
                    style={{ fontSize: 13, color: '#64748b', lineHeight: 1.65 }}>
                    Upload raw ERP tables  and let the system
                    automatically build the process event log.
                  </motion.div>
                )}
              </AnimatePresence>
            </motion.div>
            <motion.button layout
              onClick={e => { e.stopPropagation(); onGoTableBuild(); }}
              style={{
                background: '#0078D4', color: '#fff', border: 'none', padding: '11px 28px',
                borderRadius: 7, fontSize: 13, fontWeight: 700, cursor: 'pointer', width: '100%', marginTop: 'auto'
              }}
              onMouseOver={e => e.currentTarget.style.background = '#084f86ff'}
              onMouseOut={e => e.currentTarget.style.background = '#056ebdff'}>
              {hoveredSide === 'csv' ? 'Build' : 'Build Event Log'}
            </motion.button>
          </motion.div>

          {/* Upload Pre-built CSV panel */}
          <motion.div
            layout
            style={{
              flex: hoveredSide === 'csv' ? 1.7 : (hoveredSide === 'build' ? 0.6 : 1),
              background: '#fff', border: '2px solid #E2E8F0', borderRadius: 16, padding: '28px 24px',
              display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 14, textAlign: 'center',
              boxShadow: '0 4px 12px rgba(0,0,0,0.03)', cursor: 'pointer', overflow: 'hidden'
            }}
            onClick={onGoCsvUpload}
            onMouseEnter={() => setHoveredSide('csv')}
            onMouseLeave={() => setHoveredSide(null)}
            animate={{ borderColor: hoveredSide === 'csv' ? '#0078D4' : '#E2E8F0' }}
          >
            <motion.div layout style={{
              width: 60, height: 60, borderRadius: 14, background: '#EFF6FF',
              display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0
            }}>
              <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="#0078D4" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round">
                <path d="M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z" />
                <polyline points="14 2 14 8 20 8" />
                <path d="M8 13h8M8 17h8" />
              </svg>
            </motion.div>
            <motion.div layout style={{ flex: 1 }}>
              <motion.div layout style={{ fontSize: 18, fontWeight: 700, color: '#1e293b', marginBottom: 8, whiteSpace: hoveredSide === 'build' ? 'nowrap' : 'normal' }}>Upload Pre-built CSV or Excel</motion.div>
              <AnimatePresence>
                {hoveredSide !== 'build' && (
                  <motion.div
                    initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
                    style={{ fontSize: 13, color: '#64748b', lineHeight: 1.65 }}>
                    Already have a formatted event log? Upload your pre-built wide-format CSV or Excel directly to launch the dashboard.
                  </motion.div>
                )}
              </AnimatePresence>
            </motion.div>
            <motion.button layout
              onClick={e => { e.stopPropagation(); onGoCsvUpload(); }}
              style={{
                background: '#0078D4', color: '#fff', border: 'none', padding: '11px 28px',
                borderRadius: 7, fontSize: 13, fontWeight: 700, cursor: 'pointer', width: '100%', marginTop: 'auto'
              }}
              onMouseOver={e => e.currentTarget.style.background = '#005A9E'}
              onMouseOut={e => e.currentTarget.style.background = '#0078D4'}>
              {hoveredSide === 'build' ? 'Upload' : 'Upload Pre-built CSV or Excel'}
            </motion.button>
          </motion.div>
        </div>
      </div>
    </div>
  );
};

/* ── Table Upload Screen (Build Event Log path) ────────────────────────── */
const TableUploadScreen = ({ onBuilt, onBack, onLoadingChange, currentUser, myFiles, fetchingFiles, handleLoadOldFile }) => {
  const sapTables = [
    {
      name: 'EKKO', desc: 'Purchasing Document Header', isMandatory: true, required: [
        { col: 'EBELN', note: 'PO number — join key' },
        { col: 'AEDAT', note: 'PO creation date → PO Creation' },
        { col: 'BEDAT', note: 'PO document date → PO Date' },
        { col: 'BSART', note: 'Document type — filter & chart' },
        { col: 'LIFNR', note: 'Vendor ID — filter & chart' },
        { col: 'BUKRS', note: 'Company code — filter & chart' },
        { col: 'EKGRP', note: 'Purchasing group — filter & chart' },
        { col: 'ERNAM', note: 'PO creator — SOD checks & chart' },
        { col: 'LOEKZ', note: 'Deletion flag — PO Reversal rule' },
      ]
    },
    {
      name: 'EKPO', desc: 'Purchasing Document Item', isMandatory: true, required: [
        { col: 'EBELN', note: 'PO number — join key' },
        { col: 'EBELP', note: 'PO item — part of UniqueID_PO' },
        { col: 'MATNR', note: 'Material number' },
        { col: 'WERKS', note: 'Plant — filter & chart' },
        { col: 'MATKL', note: 'Material group — filter & chart' },
        { col: 'BANFN', note: 'PR number — part of UniqueID_PR' },
        { col: 'BNFPO', note: 'PR item — part of UniqueID_PR' },
        { col: 'LOEKZ', note: 'Deletion flag — PO Reversal Date' },
        { col: 'AEDAT', note: 'Item change date' },
        { col: 'MENGE', note: 'Quantity' },
        { col: 'NETPR', note: 'Net Price' },
        { col: 'NETWR', note: 'Net Value' },
        { col: 'BRTWR', note: 'Gross Value' },
      ]
    },
    {
      name: 'EBAN', desc: 'Purchase Requisition', required: [
        { col: 'BANFN', note: 'PR number — join key' },
        { col: 'BNFPO', note: 'PR item — join key' },
        { col: 'BADAT', note: 'PR requirement date → PR Creation' },
        { col: 'FRGDT', note: 'PR release date → PR Release Date' },
        { col: 'ERNAM', note: 'PR creator — SOD checks & chart' },
        { col: 'ERDAT', note: 'PR creation date — PR Reversal rule' },
        { col: 'LOEKZ', note: 'Deletion flag — PR Reversal rule' },
      ]
    },
    {
      name: 'EKBE', desc: 'PO History / GR & Invoice Events', required: [
        { col: 'EBELN', note: 'PO number — join key' },
        { col: 'EBELP', note: 'PO item — join key' },
        { col: 'VGABE', note: 'Movement type: 1=GR, 2=Invoice' },
        { col: 'BUDAT', note: 'Posting date → GR / Invoice dates' },
        { col: 'SHKZG', note: 'Debit/Credit: S=normal, H=reversal' },
        { col: 'ERNAM', note: 'Posting user — GR/Invoice Creation User' },
        { col: 'BELNR', note: 'Accounting document number' },
        { col: 'GJAHR', note: 'Fiscal year' },
        { col: 'MENGE', note: 'Quantity' },
        { col: 'DMBTR', note: 'Amount in local currency' },
        { col: 'WRBTR', note: 'Amount in document currency' },
      ]
    },
    {
      name: 'LFA1', desc: 'Vendor Master — General', required: [
        { col: 'LIFNR', note: 'Vendor ID — join key' },
        { col: 'NAME1', note: 'Vendor name — vendor filter & chart' },
      ]
    },
  ];

  const oracleTables = [
    {
      name: 'POR_REQUISITION_ALL', desc: 'Purchase Requisitions', isMandatory: true, required: [
        { col: 'REQUISITION_HEADER_ID', note: 'Requisition header identifier' },
        { col: 'REQUISITION_LINE_ID', note: 'Requisition line identifier' },
        { col: 'CREATION_DATE', note: 'PR creation date' },
        { col: 'PREPARER_ID', note: 'PR creator' }
      ]
    },
    {
      name: 'PO_HEADER_ALL / PO_LINES_ALL', desc: 'Purchase Order Headers & Lines', isMandatory: true, required: [
        { col: 'PO_HEADER_ID', note: 'PO header identifier — join key' },
        { col: 'PO_LINE_ID', note: 'PO line identifier' },
        { col: 'SEGMENT1', note: 'PO number' },
        { col: 'CREATION_DATE', note: 'PO creation date' },
        { col: 'VENDOR_ID', note: 'Vendor ID' },
        { col: 'QUANTITY', note: 'Ordered quantity' },
        { col: 'UNIT_PRICE', note: 'Unit price' }
      ]
    },
    {
      name: 'RCV_TRANSACTIONS / RCV_SHIPMENT_HEADERS', desc: 'Receipt Headers & Transactions', required: [
        { col: 'TRANSACTION_ID', note: 'Transaction identifier' },
        { col: 'SHIPMENT_HEADER_ID', note: 'Shipment header identifier' },
        { col: 'PO_HEADER_ID', note: 'PO header identifier' },
        { col: 'TRANSACTION_TYPE', note: 'Movement type (Receive/Deliver)' },
        { col: 'TRANSACTION_DATE', note: 'Transaction posting date' },
        { col: 'QUANTITY', note: 'Received quantity' }
      ]
    },
    {
      name: 'AP_INVOICES_ALL', desc: 'Invoice Headers', required: [
        { col: 'INVOICE_ID', note: 'Invoice identifier' },
        { col: 'INVOICE_NUM', note: 'Invoice number' },
        { col: 'INVOICE_DATE', note: 'Invoice date' },
        { col: 'INVOICE_AMOUNT', note: 'Invoice amount' }
      ]
    },
    {
      name: 'AP_INVOICE_PAUMENTS_ALL', desc: 'Invoice Payments', required: [
        { col: 'INVOICE_PAYMENT_ID', note: 'Payment identifier' },
        { col: 'INVOICE_ID', note: 'Invoice identifier' },
        { col: 'PAYMENT_DATE', note: 'Payment date' },
        { col: 'PAYMENT_AMOUNT', note: 'Payment amount' }
      ]
    },
    {
      name: 'XLA_AE_HEADERS', desc: 'Subledger Journal Entries', required: [
        { col: 'AE_HEADER_ID', note: 'Accounting entry header ID' },
        { col: 'ACCOUNTING_DATE', note: 'Accounting date' }
      ]
    },
    {
      name: 'POZ_SUPPLIERS', desc: 'Suppliers Master', required: [
        { col: 'VENDOR_ID', note: 'Vendor ID — join key' },
        { col: 'VENDOR_NAME', note: 'Vendor name' }
      ]
    }
  ];

  const d365Tables = [
    {
      name: 'PurchReqTable', desc: 'Purchase Requisitions', isMandatory: true, required: [
        { col: 'PurchReqId', note: 'Requisition identifier' },
        { col: 'CreatedDateTime', note: 'PR creation date' },
        { col: 'CreatedBy', note: 'PR creator' }
      ]
    },
    {
      name: 'PurchTable / PurchLine', desc: 'Purchase Order Headers & Lines', isMandatory: true, required: [
        { col: 'PurchId', note: 'PO number — join key' },
        { col: 'LineNum', note: 'Line number' },
        { col: 'ItemId', note: 'Material identifier' },
        { col: 'Qty', note: 'PO quantity' },
        { col: 'PurchPrice', note: 'Purch price' }
      ]
    },
    {
      name: 'VendPackingSlipJour / VendPackingSlipTrans', desc: 'Vendor Product Receipts', required: [
        { col: 'PackingSlipId', note: 'Packing slip identifier' },
        { col: 'DeliveryDate', note: 'Receipt posting date' },
        { col: 'Qty', note: 'Received quantity' }
      ]
    },
    {
      name: 'VendInvoiceJour / VendInvoiceTrans', desc: 'Vendor Invoices', required: [
        { col: 'InvoiceId', note: 'Invoice identifier' },
        { col: 'InvoiceDate', note: 'Invoice date' },
        { col: 'LineAmount', note: 'Invoice amount' }
      ]
    },
    {
      name: 'VendTrans', desc: 'Vendor Ledger Transactions', required: [
        { col: 'AccountNum', note: 'Vendor account ID' },
        { col: 'TransDate', note: 'Payment Date' },
        { col: 'AmountMST', note: 'Transaction amount' }
      ]
    },
    {
      name: 'LedgerJournalTrans', desc: 'General Ledger Journal Lines', required: [
        { col: 'JournalNum', note: 'Journal identifier' },
        { col: 'TransDate', note: 'Clearing date' }
      ]
    },
    {
      name: 'VendTable', desc: 'Vendors Master', required: [
        { col: 'AccountNum', note: 'Vendor account ID — join key' },
        { col: 'Party', note: 'Vendor organization name' }
      ]
    }
  ];
  const zohoTables = [
    {
      name: 'Purchase Requisitions', isMandatory: true, required: [
        { col: 'PurchReqId', note: 'PR number / Requisition identifier — join key' },
        { col: 'PRLineNum', note: 'PR item number — join key' },
        { col: 'CreatedDateTime', note: 'PR requirement date' },
        { col: 'ReleaseDate', note: 'PR release date' },
        { col: 'CreatedBy', note: 'PR creator' },
        { col: 'CreationDate', note: 'PR creation date' },
        { col: 'DeletionFlag', note: 'PR deletion flag' },
      ]
    },
    {
      name: 'Purchasing Document Headers / Purchasing Document Items', isMandatory: true, required: [
        { col: 'PurchId', note: 'PO number — join key' },
        { col: 'LineNum', note: 'PO line number — join key' },
        { col: 'ItemId', note: 'Material identifier' },
        { col: 'Plant', note: 'Plant' },
        { col: 'MaterialGroup', note: 'Material group' },
        { col: 'PurchReqId', note: 'PR number' },
        { col: 'PRLineNum', note: 'PR item number' },
        { col: 'DeletionFlag', note: 'PO deletion flag' },
        { col: 'POChangeDate', note: 'PO creation/change date' },
        { col: 'CreatedDateTime', note: 'PO document date' },
        { col: 'DocType', note: 'Document type' },
        { col: 'VendorId', note: 'Vendor ID' },
        { col: 'CompanyCode', note: 'Company code' },
        { col: 'PurchGroup', note: 'Purchasing group' },
        { col: 'CreatedBy', note: 'PO creator' },
        { col: 'Qty', note: 'PO quantity' },
        { col: 'PurchPrice', note: 'Net Price' },
        { col: 'NetValue', note: 'Net Value' },
        { col: 'GrossValue', note: 'Gross Value' },
      ]
    },
    {
      name: 'Goods Receipts/ Invoice Receipt events', required: [
        { col: 'PurchId', note: 'PO number — join key' },
        { col: 'LineNum', note: 'PO line number — join key' },
        { col: 'MovementType', note: 'Movement type: 1=GR, 2=Invoice' },
        { col: 'PostingDate', note: 'Posting date' },
        { col: 'DebitCredit', note: 'Debit/Credit: S=normal, H=reversal' },
        { col: 'PostingUser', note: 'Posting user' },
        { col: 'PackingSlipId', note: 'Accounting document number' },
        { col: 'FiscalYear', note: 'Fiscal year' },
        { col: 'Qty', note: 'Quantity' },
        { col: 'LocalAmount', note: 'Amount in local currency' },
        { col: 'DocAmount', note: 'Amount in document currency' },
      ]
    },
    {
      name: 'Vendor Master', required: [
        { col: 'VendorId', note: 'Vendor ID — join key' },
        { col: 'VendorName', note: 'Vendor name' },
      ]
    },
  ];

  const othererpTables = [
    {
      name: 'Purchase Requisitions', isMandatory: true, required: [
        { col: 'PurchReqId', note: 'PR number / Requisition identifier — join key' },
        { col: 'PRLineNum', note: 'PR item number — join key' },
        { col: 'CreatedDateTime', note: 'PR requirement date' },
        { col: 'ReleaseDate', note: 'PR release date' },
        { col: 'CreatedBy', note: 'PR creator' },
        { col: 'CreationDate', note: 'PR creation date' },
        { col: 'DeletionFlag', note: 'PR deletion flag' },
      ]
    },
    {
      name: 'Purchasing Document Headers / Purchasing Document Items', isMandatory: true, required: [
        { col: 'PurchId', note: 'PO number — join key' },
        { col: 'LineNum', note: 'PO line number — join key' },
        { col: 'ItemId', note: 'Material identifier' },
        { col: 'Plant', note: 'Plant' },
        { col: 'MaterialGroup', note: 'Material group' },
        { col: 'PurchReqId', note: 'PR number' },
        { col: 'PRLineNum', note: 'PR item number' },
        { col: 'DeletionFlag', note: 'PO deletion flag' },
        { col: 'POChangeDate', note: 'PO creation/change date' },
        { col: 'CreatedDateTime', note: 'PO document date' },
        { col: 'DocType', note: 'Document type' },
        { col: 'VendorId', note: 'Vendor ID' },
        { col: 'CompanyCode', note: 'Company code' },
        { col: 'PurchGroup', note: 'Purchasing group' },
        { col: 'CreatedBy', note: 'PO creator' },
        { col: 'Qty', note: 'PO quantity' },
        { col: 'PurchPrice', note: 'Net Price' },
        { col: 'NetValue', note: 'Net Value' },
        { col: 'GrossValue', note: 'Gross Value' },
      ]
    },
    {
      name: 'Goods Receipts/ Invoice Receipt events', required: [
        { col: 'PurchId', note: 'PO number — join key' },
        { col: 'LineNum', note: 'PO line number — join key' },
        { col: 'MovementType', note: 'Movement type: 1=GR, 2=Invoice' },
        { col: 'PostingDate', note: 'Posting date' },
        { col: 'DebitCredit', note: 'Debit/Credit: S=normal, H=reversal' },
        { col: 'PostingUser', note: 'Posting user' },
        { col: 'PackingSlipId', note: 'Accounting document number' },
        { col: 'FiscalYear', note: 'Fiscal year' },
        { col: 'Qty', note: 'Quantity' },
        { col: 'LocalAmount', note: 'Amount in local currency' },
        { col: 'DocAmount', note: 'Amount in document currency' },
      ]
    },
    {
      name: 'Vendor Master', required: [
        { col: 'VendorId', note: 'Vendor ID — join key' },
        { col: 'VendorName', note: 'Vendor name' },
      ]
    },
  ];

  const [erpSystem, setErpSystem] = useState('');

  const getTablesForSystem = (sys) => {
    if (sys === 'Oracle') return oracleTables;
    if (sys === 'Microsoft Dynamic 365') return d365Tables;
    if (sys === 'Zoho') return zohoTables;
    if (sys === 'Others') return othererpTables;
    return sapTables;
  };

  const tables = getTablesForSystem(erpSystem);
  const allPossibleTables = sapTables.concat(oracleTables).concat(d365Tables).concat(zohoTables).concat(othererpTables);

  const [tableStatus, setTableStatus] = useState(Object.fromEntries(allPossibleTables.map(t => [t.name, 'idle'])));
  const [tableMsg, setTableMsg] = useState(Object.fromEntries(allPossibleTables.map(t => [t.name, ''])));
  const [building, setBuilding] = useState(false);
  const [buildMsg, setBuildMsg] = useState('');
  const [colMapping, setColMapping] = useState(null); // {tableName, file, tableDef, uploadedCols:[], mapping:{}}
  const [tableCols, setTableCols] = useState({});
  const [selectedFiles, setSelectedFiles] = useState({});
  const [appliedMappings, setAppliedMappings] = useState({});

  const getBackendTableName = (frontName) => {
    if (frontName === 'Purchase Requisitions') return 'EBAN';
    if (frontName === 'Goods Receipts/ Invoice Receipt events') return 'EKBE';
    if (frontName === 'Vendor Master') return 'LFA1';
    return frontName;
  };

  const getBackendColumnMapping = (tableName, userMapping) => {
    const translation = {
      'PurchReqId': 'BANFN',
      'PRLineNum': 'BNFPO',
      'CreatedDateTime': 'BADAT',
      'ReleaseDate': 'FRGDT',
      'CreatedBy': 'ERNAM',
      'CreationDate': 'ERDAT',
      'DeletionFlag': 'LOEKZ',
      'PurchId': 'EBELN',
      'LineNum': 'EBELP',
      'ItemId': 'MATNR',
      'Plant': 'WERKS',
      'MaterialGroup': 'MATKL',
      'POChangeDate': 'AEDAT',
      'DocType': 'BSART',
      'VendorId': 'LIFNR',
      'CompanyCode': 'BUKRS',
      'PurchGroup': 'EKGRP',
      'Qty': 'MENGE',
      'PurchPrice': 'NETPR',
      'NetValue': 'NETWR',
      'GrossValue': 'BRTWR',
      'MovementType': 'VGABE',
      'PostingDate': 'BUDAT',
      'DebitCredit': 'SHKZG',
      'PostingUser': 'ERNAM',
      'PackingSlipId': 'BELNR',
      'FiscalYear': 'GJAHR',
      'LocalAmount': 'DMBTR',
      'DocAmount': 'WRBTR',
      'VendorName': 'NAME1'
    };

    const genericTranslation = {
      'PurchId': 'EBELN',
      'LineNum': 'EBELP',
      'ItemId': 'MATNR',
      'Qty': 'MENGE',
      'PurchPrice': 'NETPR',
      'PackingSlipId': 'BELNR',
      'DeliveryDate': 'BUDAT',
      'AccountNum': 'LIFNR',
      'VendorName': 'NAME1',
      'PurchReqId': 'BANFN',
      'CreatedDateTime': 'BADAT',
      'CreatedBy': 'ERNAM',
    };

    const finalMapping = {};
    Object.entries(userMapping).forEach(([fileCol, reqCol]) => {
      const sapCol = translation[reqCol] || genericTranslation[reqCol] || reqCol;
      finalMapping[fileCol] = sapCol;
    });
    return finalMapping;
  };

  const fileRefs = useRef(Object.fromEntries(allPossibleTables.map(t => [t.name, React.createRef()])));

  const allDone = tables.filter(t => t.isMandatory).every(t => tableStatus[t.name] === 'done');
  const anyUploading = tables.some(t => tableStatus[t.name] === 'uploading') || building;

  // ── Clear server-side tables on mount so user always starts fresh ──────────
  React.useEffect(() => {
    fetch(`${API}/p2p/transform/status?username=${encodeURIComponent(currentUser || 'Unknown')}`)
      .then(r => r.ok ? r.json() : null)
      .then(d => {
        if (!d || !d.loaded) return;
        // Clear any leftover tables from previous session
        d.loaded.forEach(tName => {
          fetch(`${API}/p2p/transform/clear_table?table_name=${tName}&username=${encodeURIComponent(currentUser || 'Unknown')}`, { method: 'DELETE' }).catch(() => { });
        });
      }).catch(() => { });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const uploadTable = async (tableName, file) => {
    if (!file) return;
    const ext = file.name.toLowerCase();
    if (!ext.endsWith('.csv') && !ext.endsWith('.xlsx') && !ext.endsWith('.xls')) {
      setTableStatus(p => ({ ...p, [tableName]: 'error' }));
      setTableMsg(p => ({ ...p, [tableName]: 'Only .csv or Excel files accepted.' }));
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
      const rPrev = await fetch(`${API}/p2p/transform/preview_columns`, { method: 'POST', body: formPreview });
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

    const backendMapping = getBackendColumnMapping(tableName, mapping);

    const uploadSingle = async (backendName) => {
      const form = new FormData();
      form.append('file', file);
      form.append('table_name', backendName);
      form.append('username', currentUser || 'Unknown');
      form.append('column_mapping', JSON.stringify(backendMapping));
      const r = await fetch(`${API}/p2p/transform/upload_table`, { method: 'POST', body: form });
      const d = await r.json();
      if (!r.ok) throw new Error(d.detail || `HTTP ${r.status}`);
      return d;
    };

    try {
      if (tableName === 'Purchasing Document Headers / Purchasing Document Items') {
        const d1 = await uploadSingle('EKKO');
        const d2 = await uploadSingle('EKPO');
        setTableStatus(p => ({ ...p, [tableName]: 'done' }));
        setTableMsg(p => ({ ...p, [tableName]: `${Number(d1.rows).toLocaleString()} rows` }));
      } else {
        const backendName = getBackendTableName(tableName);
        const d = await uploadSingle(backendName);
        setTableStatus(p => ({ ...p, [tableName]: 'done' }));
        setTableMsg(p => ({ ...p, [tableName]: `${Number(d.rows).toLocaleString()} rows` }));
        if (d.columns) {
          setTableCols(p => ({ ...p, [tableName]: d.columns }));
        }
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
      const r = await fetch(`${API}/p2p/transform/build?username=${encodeURIComponent(currentUser || 'Unknown')}`, { method: 'POST' });
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
      // Extract the faulty table name if possible
      const match = e.message.match(/incorrect for (\w+)/);
      const tableName = match ? match[1] : null;
      if (tableName) {
        setTableStatus(p => ({ ...p, [tableName]: 'error' }));
        setTableMsg(p => ({ ...p, [tableName]: e.message }));
      }

      // Always navigate to dashboard on build failure per user request
      onBuilt && onBuilt(e.message, 'table');
    }
  };

  const handleClearAll = async () => {
    if (!window.confirm("Are you sure you want to clear all uploaded tables?")) return;
    try {
      const loadedTables = Object.keys(tableStatus).filter(t => tableStatus[t] === 'done' || tableStatus[t] === 'error');
      for (const tName of loadedTables) {
        if (tName === 'Purchasing Document Headers / Purchasing Document Items') {
          await fetch(`${API}/p2p/transform/clear_table?table_name=EKKO&username=${encodeURIComponent(currentUser || 'Unknown')}`, { method: 'DELETE' });
          await fetch(`${API}/p2p/transform/clear_table?table_name=EKPO&username=${encodeURIComponent(currentUser || 'Unknown')}`, { method: 'DELETE' });
        } else {
          await fetch(`${API}/p2p/transform/clear_table?table_name=${getBackendTableName(tName)}&username=${encodeURIComponent(currentUser || 'Unknown')}`, { method: 'DELETE' });
        }
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
    if (s === 'uploading') return { icon: '…', color: '#0078D4', bg: '#EFF6FF', border: '#0078D4' };
    return { icon: '↑', color: '#0078D4', bg: '#fff', border: '#0078D4' };
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

                  const clearTarget = async (backendName) => {
                    await fetch(`${API}/p2p/transform/clear_table?table_name=${backendName}&username=${encodeURIComponent(currentUser || 'Unknown')}`, { method: 'DELETE' }).catch(console.error);
                  };
                  if (colMapping.tableDef.name === 'Purchasing Document Headers / Purchasing Document Items') {
                    await clearTarget('EKKO');
                    await clearTarget('EKPO');
                  } else {
                    await clearTarget(getBackendTableName(colMapping.tableDef.name));
                  }

                  setAppliedMappings(p => ({ ...p, [colMapping.tableDef.name]: finalMapping }));
                  performUpload(colMapping.tableDef.name, colMapping.file, finalMapping);
                }}
                style={{ padding: '8px 16px', background: '#0078D4', color: '#fff', border: 'none', borderRadius: 6, fontSize: 13, fontWeight: 700, cursor: 'pointer' }}>
                Confirm Mapping & Upload
              </button>
            </div>
          </div>
        </div>
      )}

      <div style={{ maxWidth: 820, width: '100%', display: 'flex', flexDirection: 'column', gap: 20 }}>
        {/* Back + header */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <div>
            <div style={{ fontSize: 11, fontWeight: 700, color: '#0078D4', textTransform: 'uppercase', letterSpacing: 0.8 }}>Build Event Log</div>
            <div style={{ fontSize: 13, color: '#64748b' }}>Upload ERP tables below, then click Build.</div>
          </div>
        </div>

        {/* Table upload panel */}
        <div style={{ background: '#fff', border: '1px solid #E2E8F0', borderRadius: 10, padding: '20px 22px', boxShadow: '0 2px 6px rgba(0,0,0,0.04)' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 14 }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: 0.8 }}>
              {erpSystem ? `${erpSystem} Tables` : 'ERP Tables'}
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              <select
                value={erpSystem}
                onChange={e => setErpSystem(e.target.value)}
                style={{
                  padding: '5px 12px',
                  borderRadius: 6,
                  border: '1.5px solid #E2E8F0',
                  background: '#fff',
                  fontSize: 12,
                  fontWeight: 600,
                  color: '#334155',
                  cursor: 'pointer',
                  outline: 'none',
                  boxShadow: '0 1px 2px rgba(0,0,0,0.05)'
                }}
              >
                <option value="">Select ERP System</option>
                <option value="SAP">SAP</option>
                <option value="Oracle">Oracle</option>
                <option value="Microsoft Dynamic 365">Microsoft Dynamic 365</option>
                <option value="Zoho">Zoho</option>
                <option value="Others">Others</option>
              </select>
              <div style={{ fontSize: 11, color: '#94a3b8' }}>Upload each as <strong style={{ color: '#475569' }}>.csv or Excel</strong></div>
            </div>
          </div>
          {['SAP', 'Oracle', 'Microsoft Dynamic 365', 'Zoho', 'Others'].includes(erpSystem) ? (
            <motion.div
              key={erpSystem}
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
                      backgroundColor: ['#F8FAFC', '#EFF6FF', '#F8FAFC'],
                      transition: { duration: 1.5, repeat: Infinity }
                    } : {}}
                    style={{
                      display: 'flex', alignItems: 'center', gap: 12, padding: '10px 14px',
                      background: tableStatus[t.name] === 'done' ? '#F0FAF0' : tableStatus[t.name] === 'error' ? '#FDE7E9' : i % 2 === 0 ? '#F8FAFC' : '#fff',
                      borderBottom: i < tables.length - 1 ? '1px solid #E2E8F0' : 'none', transition: 'background 0.2s'
                    }}>
                    <input ref={ref} type="file" accept=".csv,.xlsx,.xls" style={{ display: 'none' }}
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
                      onMouseOver={e => { if (!isUp) e.currentTarget.style.background = '#DBEAFE'; }}
                      onMouseOut={e => { e.currentTarget.style.background = s.bg; }}>
                      {isUp ? <span style={{ animation: 'spin 1s linear infinite', display: 'inline-block' }}>↻</span> : s.icon}
                    </button>
                    <div style={{
                      minWidth: 52, fontFamily: 'monospace', fontWeight: 700, fontSize: 13, color: '#0078D4',
                      background: '#EFF6FF', padding: '3px 8px', borderRadius: 4, textAlign: 'center', flexShrink: 0
                    }}>{t.isMandatory ? '* ' : ''}{t.name}</div>
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
                                background: tableStatus[t.name] === 'error' ? '#FEF2F2' : '#EFF6FF',
                                color: tableStatus[t.name] === 'error' ? '#DC2626' : '#1D4ED8',
                                border: tableStatus[t.name] === 'error' ? '1px solid #FCA5A5' : '1px solid #93C5FD',
                                cursor: 'pointer'
                              }}
                              onMouseOver={e => e.currentTarget.style.background = tableStatus[t.name] === 'error' ? '#FECACA' : '#DBEAFE'}
                              onMouseOut={e => e.currentTarget.style.background = tableStatus[t.name] === 'error' ? '#FEF2F2' : '#EFF6FF'}>
                              Map Columns
                            </button>
                          )}
                          <button
                            onClick={async () => {
                              const clearTarget = async (backendName) => {
                                await fetch(`${API}/p2p/transform/clear_table?table_name=${backendName}&username=${encodeURIComponent(currentUser || 'Unknown')}`, { method: 'DELETE' }).catch(console.error);
                              };
                              if (t.name === 'Purchasing Document Headers / Purchasing Document Items') {
                                await clearTarget('EKKO');
                                await clearTarget('EKPO');
                              } else {
                                await clearTarget(getBackendTableName(t.name));
                              }
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
          ) : (
            <div style={{
              display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center',
              padding: '40px 20px', border: '1px dashed #E2E8F0', borderRadius: 8, background: '#F8FAFC',
              color: '#64748b', gap: 10
            }}>
              <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" style={{ opacity: 0.6 }}>
                <rect x="3" y="3" width="18" height="18" rx="2" ry="2" />
                <line x1="9" y1="3" x2="9" y2="21" />
                <line x1="15" y1="3" x2="15" y2="21" />
                <line x1="3" y1="9" x2="21" y2="9" />
                <line x1="3" y1="15" x2="21" y2="15" />
              </svg>
              <div style={{ fontSize: 13, fontWeight: 600 }}>
                {erpSystem ? `No tables configured for ${erpSystem} yet.` : 'Select an ERP System to load required tables'}
              </div>
              <div style={{ fontSize: 11, color: '#94a3b8' }}>
              </div>
            </div>
          )}
          <div style={{ marginTop: 16, display: 'flex', alignItems: 'center', gap: 12, justifyContent: 'space-between', flexWrap: 'wrap' }}>
            <div style={{ fontSize: 11, color: '#64748b' }}>
              <span style={{ fontWeight: 700, color: '#DC2626' }}>*</span> Mandatory table
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
              {buildMsg && <div style={{ fontSize: 12, color: buildMsg.startsWith('Error') ? '#D13438' : '#107C10', fontWeight: 600 }}>{buildMsg}</div>}
              {buildMsg && !buildMsg.startsWith('Error') && (
                <button onClick={() => { const url = `${API}/p2p/download_output?username=${encodeURIComponent(currentUser || 'Unknown')}`; const a = document.createElement('a'); a.href = url; a.download = ''; a.click(); }}
                  style={{ background: '#006B3C', color: '#fff', border: 'none', padding: '5px 14px', borderRadius: 4, fontSize: 12, fontWeight: 700, cursor: 'pointer' }}
                  onMouseOver={e => e.currentTarget.style.background = '#004d2c'}
                  onMouseOut={e => e.currentTarget.style.background = '#006B3C'}>
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
                  background: allDone && !anyUploading ? '#006B3C' : '#A8A8A8', color: '#fff', border: 'none',
                  padding: '10px 28px', borderRadius: 6, fontSize: 13, fontWeight: 700,
                  cursor: allDone && !anyUploading ? 'pointer' : 'not-allowed', whiteSpace: 'nowrap',
                  boxShadow: allDone && !anyUploading ? '0 2px 8px rgba(0,107,60,0.3)' : 'none'
                }}
                onMouseOver={e => { if (allDone && !anyUploading) e.currentTarget.style.background = '#004d2c'; }}
                onMouseOut={e => { e.currentTarget.style.background = allDone && !anyUploading ? '#006B3C' : '#A8A8A8'; }}>
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
                        <span style={{ fontSize: 9, fontWeight: 700, padding: '2px 7px', borderRadius: 10, background: '#1d74a9ff', color: '#fffefeff', border: '1px solid #A8D5B5' }}>Table Build</span>
                      </td>
                      <td style={{ padding: '9px 14px', color: '#64748b', whiteSpace: 'nowrap', fontSize: 11 }}>{f.upload_date}</td>
                      <td style={{ padding: '9px 14px', color: '#1e293b', fontWeight: 600, textAlign: 'right' }}>{f.cases != null ? Number(f.cases).toLocaleString() : '—'}</td>
                      <td style={{ padding: '9px 14px', color: '#64748b', textAlign: 'right' }}>{f.rows != null ? Number(f.rows).toLocaleString() : '—'}</td>
                      <td style={{ padding: '9px 14px' }}>
                        <button onClick={() => handleLoadOldFile && handleLoadOldFile(f.file_id)}
                          style={{ background: '#0078D4', color: '#fff', border: 'none', padding: '5px 12px', borderRadius: 4, cursor: 'pointer', fontSize: 11, fontWeight: 600 }}
                          onMouseOver={e => e.currentTarget.style.background = '#0b5d9cff'}
                          onMouseOut={e => e.currentTarget.style.background = '#0078d4ff'}>
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


const UploadBanner = React.memo(({ onUploaded, serverOk, onLoadingChange, currentUser, myFiles, fetchingFiles, handleLoadOldFile, step, setStep, introStep, setIntroStep }) => {
  // 'info' = landing info page, 'table' = SAP table upload, 'upload' = pre-built CSV
  const [dragging, setDragging] = useState(false);
  const [status, setStatus] = useState('idle');
  const [msg, setMsg] = useState('');
  const [selectedFile, setSelectedFile] = useState(null);
  const [colMapping, setColMapping] = useState(null);
  const inputRef = useRef();

  const findBestMatch = (reqCol, uploadedCols) => {
    const clean = (s) => s.toUpperCase().replace(/[^A-Z0-9]/g, '');
    const reqClean = clean(reqCol);

    let match = uploadedCols.find(c => c.toUpperCase() === reqCol.toUpperCase());
    if (match) return match;

    match = uploadedCols.find(c => clean(c) === reqClean);
    if (match) return match;

    const aliases = {
      'UniqueID_PO': ['UNIQUEID_PO', 'UNIQUEID', 'EBELN'],
      'PO Qty': ['MENGE', 'PO_QTY', 'POQTY', 'QUANTITY'],
      'Netpr': ['NETPR', 'PRICE', 'NET_PRICE'],
      'Netwr': ['NETWR', 'NET_VALUE', 'NETVALUE'],
      'GR Quantity': ['MENGE (GRN)', 'GR_QTY', 'GRN_QTY', 'GRQTY', 'GRNQuantity', 'GR Quantity'],
      'GR Doc Amount': ['DMBTR', 'GR_AMOUNT', 'GRN_AMOUNT', 'GRAMOUNT', 'GRDocAmount', 'GR Doc Amount'],
      'IR Quantity': ['MENGE (Right)', 'IR_QTY', 'IRQTY', 'Invoice Qty', 'Invoice Quantity', 'IR Quantity'],
      'IR Doc Amount': ['DMBTR (Right)', 'IR_AMOUNT', 'IRAMOUNT', 'Invoice Amount', 'IR Doc Amount'],
      'LIFNR': ['VENDOR', 'VENDOR_ID', 'LIFNR'],
      'NAME1': ['NAME1', 'VENDOR_NAME', 'NAME'],
      'BUKRS': ['BUKRS', 'COMPANY', 'COMPANY_CODE'],
      'BSART': ['BSART', 'DOC_TYPE', 'DOCUMENT_TYPE'],
      'MATKL': ['MATKL', 'MATERIAL_GROUP'],
      'EKGRP': ['EKGRP', 'PURCHASING_GROUP'],
      'WERKS': ['WERKS', 'PLANT'],
      'ERNAM': ['ERNAM', 'PO_CREATOR', 'CREATED_BY']
    };

    const list = aliases[reqCol];
    if (list) {
      for (const alias of list) {
        const aliasClean = clean(alias);
        const found = uploadedCols.find(c => clean(c) === aliasClean || c.toUpperCase().includes(alias.toUpperCase()));
        if (found) return found;
      }
    }
    return '';
  };

  const doUpload = async (file, mapping = {}) => {
    if (!file) return;
    const ext = file.name.toLowerCase();
    if (!ext.endsWith('.csv') && !ext.endsWith('.xlsx') && !ext.endsWith('.xls')) { setStatus('error'); setMsg('Only .csv or Excel files accepted.'); return; }
    setStatus('uploading'); setMsg('');
    setSelectedFile(file);
    onLoadingChange(true, 10, 'Processing Data...');
    const form = new FormData();
    form.append('file', file);
    form.append('username', currentUser);
    form.append('column_mapping', JSON.stringify(mapping));
    let prog = 10;
    const ticker = setInterval(() => { prog = Math.min(prog + Math.random() * 12, 88); onLoadingChange(true, prog, 'Analysing Data...'); }, 400);
    try {
      const r = await fetch(`${API}/p2p/upload`, { method: 'POST', body: form });
      const d = await r.json();
      clearInterval(ticker);
      if (!r.ok) throw new Error(d.detail || `HTTP ${r.status}`);
      onLoadingChange(true, 100, 'Dashboard Created');
      setTimeout(() => { setStatus('done'); setMsg(`✓ ${Number(d.rows).toLocaleString()} rows · ${Number(d.unique_cases).toLocaleString()} unique cases`); onUploaded(null, 'upload'); }, 800);
    } catch (e) {
      clearInterval(ticker); onLoadingChange(false, 0, '');
      const isMappingErr = e.message.includes('Column mapping is incorrect');
      if (isMappingErr) {
        onUploaded(e.message, 'upload');
      } else {
        setStatus('error'); setMsg(`Error: ${e.message}`);
        // Navigate to dashboard on any error
        onUploaded(e.message, 'upload');
      }
    } finally {
      if (inputRef.current) inputRef.current.value = '';
    }
  };

  /* ── Page 1: Info / landing ── */
  if (step === 'info') return (
    <P2PIntroScreen
      onGoTableBuild={() => setStep('table')}
      onGoCsvUpload={() => setStep('upload')}
      currentUser={currentUser}
      introStep={introStep}
      setIntroStep={setIntroStep}
    />
  );

  /* ── Page 2: SAP Table upload → Build Event Log ── */
  if (step === 'table') return (
    <TableUploadScreen
      onBuilt={onUploaded}
      onBack={() => setStep('info')}
      onLoadingChange={onLoadingChange}
      currentUser={currentUser}
      myFiles={myFiles}
      fetchingFiles={fetchingFiles}
      handleLoadOldFile={handleLoadOldFile}
    />
  );

  /* ── Page 3: Pre-built CSV upload ── */
  if (step === 'upload') {
    const bc = dragging ? C.blue700 : status === 'done' ? '#107C10' : status === 'error' ? C.red : C.border;
    const bg = dragging ? '#EFF6FF' : status === 'done' ? '#F0FAF0' : status === 'error' ? '#FDE7E9' : '#FAFAFA';
    const SCHEMA_COLS = [
      { col: 'UniqueID_PO', desc: 'Unique PO line key (EBELN + EBELP)', req: true },
      { col: 'PO Creation', desc: 'PO creation date (AEDAT from EKKO)', req: true },
      { col: 'PO Date', desc: 'PO document date (BEDAT from EKKO)', req: true },
      { col: 'GR Posting', desc: 'Goods Receipt posting date (BUDAT/EKBE)', req: true },
      { col: 'Invoice Posting', desc: 'Invoice posting date (BUDAT/EKBE)', req: true },
      { col: 'PR Creation', desc: 'PR requirement date (BADAT from EBAN)', req: false },
      { col: 'PR Release Date', desc: 'PR release date (FRGDT from EBAN)', req: false },
      { col: 'PR Reversal Date', desc: 'PR reversal date', req: false },
      { col: 'PO Reversal Date', desc: 'PO reversal date', req: false },
      { col: 'GR Reversal Date', desc: 'GR reversal date', req: false },
      { col: 'Invoice Reversal Date', desc: 'Invoice reversal date', req: false },
      { col: 'BUKRS', desc: 'Company code', req: false },
      { col: 'LIFNR', desc: 'Vendor ID', req: false },
      { col: 'NAME1', desc: 'Vendor name (from LFA1)', req: false },
      { col: 'BSART', desc: 'Document type', req: false },
      { col: 'MATKL', desc: 'Material group', req: false },
      { col: 'EKGRP', desc: 'Purchasing group', req: false },
      { col: 'WERKS', desc: 'Plant', req: false },
      { col: 'ERNAM', desc: 'PO creator (ERNAM from EKKO)', req: false },
      { col: 'ERNAM (EBAN)', desc: 'PR creator (ERNAM from EBAN)', req: false },
      { col: 'GR Creation User', desc: 'GR posting user', req: false },
      { col: 'Invoice Creation User', desc: 'Invoice posting user', req: false },
      { col: 'PO Qty', desc: 'Purchase Order Quantity (MENGE from EKPO)', req: false },
      { col: 'Netpr', desc: 'Net Price (NETPR from EKPO)', req: false },
      { col: 'Netwr', desc: 'Net Value (NETWR from EKPO)', req: false },
      { col: 'GR Quantity', desc: 'Goods Receipt Quantity (MENGE from EKBE/GRN)', req: false },
      { col: 'GR Doc Amount', desc: 'Goods Receipt Amount (DMBTR from EKBE/GRN)', req: false },
      { col: 'IR Quantity', desc: 'Invoice Receipt Quantity (MENGE from EKBE/IR)', req: false },
      { col: 'IR Doc Amount', desc: 'Invoice Receipt Amount (DMBTR from EKBE/IR)', req: false },
    ];
    const csvUploads = (myFiles || []).filter(f => !f.source || f.source === 'csv_upload');

    const handleMapCsvColumns = async () => {
      if (!selectedFile) return;
      const formPreview = new FormData();
      formPreview.append('file', selectedFile);
      try {
        const rPrev = await fetch(`${API}/p2p/transform/preview_columns`, { method: 'POST', body: formPreview });
        const dPrev = await rPrev.json();
        if (!rPrev.ok) throw new Error(dPrev.detail || `Failed to read CSV columns`);
        setColMapping({ file: selectedFile, tableDef: { name: 'Pre-built CSV', required: SCHEMA_COLS.map(c => ({ col: c.col, note: c.desc })) }, uploadedCols: dPrev.columns, mapping: {} });
      } catch (e) {
        setStatus('error');
        setMsg(e.message);
      }
    };

    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 16, padding: '20px 14px' }}>
        {/* ══ Column Mapping Modal (CSV) ══════════════════════════════════════════════ */}
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
                <div style={{ fontSize: 12, color: '#64748b', marginTop: 3 }}> Select which columns from your file correspond to the required/optional fields.</div>
              </div>

              <div style={{ overflowY: 'auto', flex: 1, padding: '0 0 8px' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                  <thead style={{ position: 'sticky', top: 0, zIndex: 2 }}>
                    <tr style={{ background: '#F8FAFC' }}>
                      <th style={{ padding: '10px 16px', textAlign: 'left', fontWeight: 700, color: '#64748b', fontSize: 10, textTransform: 'uppercase', letterSpacing: 0.8, borderBottom: '2px solid #E2E8F0', width: '35%' }}>Column</th>
                      <th style={{ padding: '10px 12px', textAlign: 'left', fontWeight: 700, color: '#64748b', fontSize: 10, textTransform: 'uppercase', letterSpacing: 0.8, borderBottom: '2px solid #E2E8F0', width: '35%' }}>Map to File Column</th>
                      <th style={{ padding: '10px 12px', textAlign: 'left', fontWeight: 700, color: '#64748b', fontSize: 10, textTransform: 'uppercase', letterSpacing: 0.8, borderBottom: '2px solid #E2E8F0' }}>Purpose</th>
                    </tr>
                  </thead>
                  <tbody>
                    {colMapping.tableDef.required.map((r, i) => {
                      const reqCol = r.col;
                      const autoMatch = findBestMatch(reqCol, colMapping.uploadedCols);
                      const selected = colMapping.mapping[reqCol] !== undefined ? colMapping.mapping[reqCol] : (autoMatch || '');
                      return (
                        <tr key={reqCol} style={{ borderBottom: '1px solid #F1F5F9' }}>
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
                  onClick={() => {
                    const finalMapping = {};
                    colMapping.tableDef.required.forEach(r => {
                      const autoMatch = findBestMatch(r.col, colMapping.uploadedCols);
                      const sel = colMapping.mapping[r.col] !== undefined ? colMapping.mapping[r.col] : (autoMatch || '');
                      if (sel && sel !== r.col) {
                        finalMapping[sel] = r.col;
                      }
                    });
                    setColMapping(null);
                    doUpload(colMapping.file, finalMapping);
                  }}
                  style={{ padding: '8px 16px', background: '#0078D4', color: '#fff', border: 'none', borderRadius: 6, fontSize: 13, fontWeight: 700, cursor: 'pointer' }}>
                  Confirm Mapping & Upload
                </button>
              </div>
            </div>
          </div>
        )}
        {/* Back + status */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <button onClick={() => setStep('info')}
            style={{
              background: 'none', border: '1px solid #E2E8F0', padding: '5px 12px', borderRadius: 6,
              fontSize: 12, cursor: 'pointer', color: '#64748b', fontWeight: 600, flexShrink: 0
            }}
            onMouseOver={e => e.currentTarget.style.background = '#F8FAFC'}
            onMouseOut={e => e.currentTarget.style.background = 'none'}>
            ← Back
          </button>
          <div style={{ fontSize: 11, fontWeight: 700, color: '#0078D4', textTransform: 'uppercase', letterSpacing: 0.8 }}>
            Upload Pre-built CSV or Excel
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: 11, color: C.slate, marginLeft: 'auto' }}>
            <div style={{
              width: 7, height: 7, borderRadius: '50%', background: serverOk ? '#107C10' : '#D13438',
              boxShadow: serverOk ? '0 0 0 2px rgba(16,124,16,.2)' : '0 0 0 2px rgba(209,52,56,.2)'
            }} />
            {serverOk ? 'Backend connected' : 'Backend offline'}
          </div>
        </div>


        {/* Drop zone */}
        <div onDragOver={e => { e.preventDefault(); setDragging(true); }}
          onDragLeave={() => setDragging(false)}
          onDrop={e => { e.preventDefault(); setDragging(false); const f = e.dataTransfer.files[0]; if (f) { setSelectedFile(f); setStatus('idle'); setMsg(''); } }}
          onClick={() => { if (status !== 'uploading' && inputRef.current) { inputRef.current.value = ''; inputRef.current.click(); } }}
          style={{
            border: `2px dashed ${bc}`, borderRadius: 8, padding: '14px 24px', background: bg,
            cursor: 'pointer', textAlign: 'center', transition: 'all .2s',
            display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 14, flexDirection: selectedFile ? 'column' : 'row'
          }}>
          <input ref={inputRef} type="file" accept=".csv,.xlsx,.xls" style={{ display: 'none' }} onChange={e => { const f = e.target.files[0]; if (f) { setSelectedFile(f); setStatus('idle'); setMsg(''); } }} />

          <div style={{ display: 'flex', alignItems: 'center', gap: 14 }}>
            <div style={{ fontSize: 22, fontWeight: 'bold', color: status === 'done' ? '#107C10' : status === 'error' ? '#D13438' : '#0078D4' }}>
              {status === 'done' ? '✓' : status === 'error' ? '✕' : '⬆'}
            </div>
            <div style={{ textAlign: 'left' }}>
              <div style={{ fontSize: 13, fontWeight: 700, color: '#323130' }}>
                {selectedFile ? selectedFile.name : status === 'idle' ? 'Click or drag & drop a CSV or Excel file here' : status === 'done' ? 'File loaded!' : 'Upload failed'}
              </div>
              <div style={{ fontSize: 11, color: C.slate, marginTop: 2 }}>{msg || 'Wide-format or KNIME long-format CSV/Excel accepted'}</div>
            </div>
          </div>

          {selectedFile && status !== 'uploading' && (
            <div style={{ display: 'flex', gap: 12, marginTop: 4 }}>
              <button onClick={e => { e.stopPropagation(); handleMapCsvColumns(); }}
                style={{
                  fontSize: 12, padding: '8px 16px', background: '#EFF6FF', color: '#1D4ED8',
                  border: '1px solid #93C5FD', borderRadius: 6, cursor: 'pointer', fontWeight: 700
                }}
                onMouseOver={e => e.currentTarget.style.background = '#DBEAFE'}
                onMouseOut={e => e.currentTarget.style.background = '#EFF6FF'}>
                Map Columns
              </button>
              <button onClick={e => { e.stopPropagation(); doUpload(selectedFile, {}); }}
                style={{
                  fontSize: 12, padding: '8px 16px', background: '#0078D4', color: '#fff',
                  border: 'none', borderRadius: 6, cursor: 'pointer', fontWeight: 700
                }}
                onMouseOver={e => e.currentTarget.style.background = '#005A9E'}
                onMouseOut={e => e.currentTarget.style.background = '#0078D4'}>
                Upload
              </button>
              <button onClick={e => { e.stopPropagation(); setStatus('idle'); setMsg(''); setSelectedFile(null); }}
                style={{
                  fontSize: 12, padding: '8px 16px', background: '#fff',
                  border: `1px solid ${C.border}`, borderRadius: 6, cursor: 'pointer', color: C.slate
                }}>
                Clear
              </button>
            </div>
          )}
        </div>

        {/* Previous CSV Uploads */}
        <div style={{ background: '#fff', border: `1px solid ${C.border}`, borderRadius: 10, padding: '18px 20px', boxShadow: '0 2px 6px rgba(0,0,0,0.04)' }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: '#64748b', textTransform: 'uppercase', letterSpacing: 0.8, marginBottom: 12 }}>Previous CSV Uploads</div>
          {fetchingFiles ? (
            <div style={{ color: '#94a3b8', fontSize: 13 }}>Loading...</div>
          ) : csvUploads.length === 0 ? (
            <div style={{ padding: '20px', textAlign: 'center', background: '#F8FAFC', borderRadius: 8, border: '1px dashed #E2E8F0', color: '#94a3b8', fontSize: 13 }}>
              No previous CSV uploads found.
            </div>
          ) : (
            <div style={{ border: '1px solid #E2E8F0', borderRadius: 8, overflow: 'hidden' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                <thead style={{ background: '#F3F2F1', borderBottom: '1px solid #E2E8F0', textAlign: 'left' }}>
                  <tr>
                    <th style={{ padding: '9px 14px', color: '#323130', fontWeight: 600 }}>File Name</th>
                    <th style={{ padding: '9px 14px', color: '#323130', fontWeight: 600 }}>Date</th>
                    <th style={{ padding: '9px 14px', color: '#323130', fontWeight: 600, textAlign: 'right' }}>Cases</th>
                    <th style={{ padding: '9px 14px', color: '#323130', fontWeight: 600, textAlign: 'right' }}>Rows</th>
                    <th style={{ padding: '9px 14px', color: '#323130', fontWeight: 600 }}>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {csvUploads.map((f, idx) => (
                    <tr key={idx} style={{ borderBottom: '1px solid #E2E8F0', transition: 'background 0.2s' }}
                      onMouseEnter={e => e.currentTarget.style.background = '#F8FAFC'}
                      onMouseLeave={e => e.currentTarget.style.background = 'transparent'}>
                      <td style={{ padding: '9px 14px' }}>
                        <div style={{ fontWeight: 600, color: '#1e293b', fontSize: 12, wordBreak: 'break-all' }}>{f.filename}</div>
                        <span style={{ fontSize: 9, fontWeight: 700, padding: '2px 7px', borderRadius: 10, background: '#EFF6FF', color: '#0057B7', border: '1px solid #B3D1F5' }}>CSV Upload</span>
                      </td>
                      <td style={{ padding: '9px 14px', color: '#64748b', whiteSpace: 'nowrap', fontSize: 11 }}>{f.upload_date}</td>
                      <td style={{ padding: '9px 14px', color: '#1e293b', fontWeight: 600, textAlign: 'right' }}>{f.cases != null ? Number(f.cases).toLocaleString() : '—'}</td>
                      <td style={{ padding: '9px 14px', color: '#64748b', textAlign: 'right' }}>{f.rows != null ? Number(f.rows).toLocaleString() : '—'}</td>
                      <td style={{ padding: '9px 14px' }}>
                        <button onClick={() => handleLoadOldFile(f.file_id)}
                          style={{ background: '#0078D4', color: '#fff', border: 'none', padding: '5px 12px', borderRadius: 4, cursor: 'pointer', fontSize: 11, fontWeight: 600 }}
                          onMouseOver={e => e.currentTarget.style.background = '#005A9E'}
                          onMouseOut={e => e.currentTarget.style.background = '#0078D4'}>
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
    );
  }
});


const EmptyState = ({ condition, message, children, action }) => {
  if (!condition) return children;
  return (
    <div style={{
      display: 'flex', alignItems: 'center', justifyContent: 'center', height: '300px',
      background: '#F8FAFC', borderRadius: '6px', border: '1px dashed #CBD5E1', color: '#64748b', fontSize: '13px', padding: '20px', textAlign: 'center', flexDirection: 'column', gap: '12px'
    }}>
      <div style={{ fontSize: '28px', opacity: 0.8 }}>📉</div>
      <div style={{ fontWeight: 600, maxWidth: '250px' }}>{message}</div>
      {action}
    </div>
  );
};

/* ════════════════════════════════════════════
   MAIN APP
════════════════════════════════════════════ */
export default function P2PDashboard({ currentUser, onSignOut, onBackHome }) {
  const [serverOk, setServerOk] = useState(false);
  const [dataLoaded, setDataLoaded] = useState(false);
  const [mappingError, setMappingError] = useState(null);
  const [loading, setLoading] = useState(false);
  const [dashboardLoading, setDashboardLoading] = useState(true);

  const [chartsReady, setChartsReady] = useState(false);
  const [pmReady, setPmReady] = useState(false);

  const intentToUpload = useRef(true);
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
                cEl.style.setProperty('background-image', 'radial-gradient(#C8D3E8 1.5px, transparent 1.5px)', 'important');
                cEl.style.setProperty('background-size', '24px 24px', 'important');
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
          link.download = `P2P_Dashboard_Report_${new Date().toISOString().slice(0, 10)}.png`;
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
      alert('Screenshot failed: ' + err.message);
    } finally {
      // Clean up original element attributes
      originalNodes.forEach((node) => {
        node.removeAttribute('data-screenshot-id');
      });
      setScreenshotting(false);
    }
  };

  const [loadProg, setLoadProg] = useState(0);
  const [loadLabel, setLoadLabel] = useState('');
  const [filters, setFilters] = useState({});

  const [activeTab, setActiveTab] = useState('process');
  const [tabSkeleton, setTabSkeleton] = useState(false);
  const [layoutDir, setLayoutDir] = useState('TB');

  const [selected, setSelected] = useState({
    company: 'ALL', bsart: 'ALL', matkl: 'ALL', vendor: 'ALL',
    purch_group: 'ALL', case_id: 'ALL',
    month: 'ALL', quarter: 'ALL', lead_time: 'ALL', events: 'ALL', ernam: 'ALL', status: 'ALL',
    start_date: '', end_date: ''
  });
  const [crossFilter, setCrossFilter] = useState(null);
  const [hoverInfo, setHoverInfo] = useState(null);
  const hasActiveFilters = Object.entries(selected).some(([k, v]) => (k === 'start_date' || k === 'end_date') ? v !== '' : v !== 'ALL') || !!crossFilter;

  const [kpis, setKpis] = useState(null);
  const [dateRange, setDateRange] = useState({ min_date: null, max_date: null });
  const [actData, setActData] = useState([]);
  const [monData, setMonData] = useState([]);
  const [compData, setCompData] = useState([]);
  const [bsData, setBsData] = useState([]);
  const [mkData, setMkData] = useState([]);
  const [vendData, setVendData] = useState([]);
  const [ltData, setLtData] = useState([]);
  const [ernamData, setErnamData] = useState([]);
  const [caseTableData, setCaseTableData] = useState([]);
  const [caseEvents, setCaseEvents] = useState([]);

  const [poRevErnam, setPoRevErnam] = useState([]);
  const [poRevTimeline, setPoRevTimeline] = useState([]);
  const [prRevAfterPo, setPrRevAfterPo] = useState([]);
  const [seqViolation, setSeqViolation] = useState([]);
  const [happyPathData, setHappyPathData] = useState([]);

  const [sodData, setSodData] = useState([]);
  const [bottleneckData, setBottleneckData] = useState([]);
  const [revByPurchGroup, setRevByPurchGroup] = useState([]);
  const [purchGroupWorkload, setPurchGroupWorkload] = useState([]);
  const [vendorLeadTime, setVendorLeadTime] = useState([]);

  const [pmLoading, setPmLoading] = useState(false);
  const [pmError, setPmError] = useState('');

  const [rfNodes, setRfNodes, onNodesChange] = useNodesState([]);
  const [rfEdges, setRfEdges, onEdgesChange] = useEdgesState([]);
  const [rawGraphData, setRawGraphData] = useState(null);

  const [refreshTrigger, setRefreshTrigger] = useState(0);

  const [myFiles, setMyFiles] = useState([]);
  const [fetchingFiles, setFetchingFiles] = useState(false);
  const [uploadStepOverride, setUploadStepOverride] = useState(null);
  const [uploadStep, setUploadStep] = useState(uploadStepOverride || 'info');
  const [introStep, setIntroStep] = useState('overview');

  useEffect(() => {
    if (uploadStepOverride) {
      setUploadStep(uploadStepOverride);
    }
  }, [uploadStepOverride]);

  useEffect(() => {
    const current = window.history.state;
    if (current && current.activeModule === 'p2p') {
      if (current.dataLoaded !== undefined) setDataLoaded(current.dataLoaded);
      if (current.uploadStep !== undefined) setUploadStep(current.uploadStep);
      if (current.introStep !== undefined) setIntroStep(current.introStep);
    }

    const handlePopState = (event) => {
      if (event.state && event.state.activeModule === 'p2p') {
        if (event.state.dataLoaded !== undefined) setDataLoaded(event.state.dataLoaded);
        if (event.state.uploadStep !== undefined) setUploadStep(event.state.uploadStep);
        if (event.state.introStep !== undefined) setIntroStep(event.state.introStep);
      }
    };

    window.addEventListener('popstate', handlePopState);
    return () => window.removeEventListener('popstate', handlePopState);
  }, []);

  useEffect(() => {
    const current = window.history.state;
    if (!current || current.activeModule !== 'p2p' || current.dataLoaded !== dataLoaded || current.uploadStep !== uploadStep || current.introStep !== introStep) {
      window.history.pushState({ activeModule: 'p2p', dataLoaded, uploadStep, introStep }, '');
    }
  }, [dataLoaded, uploadStep, introStep]);

  useEffect(() => {
    if (currentUser && !dataLoaded) {
      setFetchingFiles(true);
      fetch(`${API}/p2p/my_files?username=${currentUser}`)
        .then(res => res.ok ? res.json() : [])
        .then(data => setMyFiles(data))
        .catch(err => console.error("Failed to fetch files", err))
        .finally(() => setFetchingFiles(false));
    }
  }, [currentUser, dataLoaded, refreshTrigger]);

  const handleLoadOldFile = async (file_id) => {
    setChartsReady(false);
    setPmReady(false);
    handleLoadingChange(true, 50, 'Loading previous dashboard...');
    try {
      const res = await fetch(`${API}/p2p/load_file`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: currentUser, file_id })
      });
      if (!res.ok) throw new Error('Failed to load file');
      intentToUpload.current = false;
      setDataLoaded(true);
      handleRefresh();
    } catch (e) {
      alert("Error loading dashboard: " + e.message);
      handleLoadingChange(false, 100, '');
    }
  };

  const getTabStyle = (isActive) => ({
    padding: '6px 16px',
    borderRadius: '20px',
    border: 'none',
    cursor: 'pointer',
    fontSize: '12px',
    fontWeight: isActive ? '700' : '600',
    background: isActive ? '#ffffff' : 'transparent',
    color: isActive ? C.headerBg : 'rgba(255,255,255,0.85)',
    boxShadow: isActive ? '0 2px 8px rgba(0,0,0,0.15)' : 'none',
    transition: 'all 0.2s',
  });

  const logAction = useCallback((action, details) => {
    if (!currentUser) return;
    fetch(`${API}/p2p/log`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: currentUser, action, details })
    }).catch(e => console.error("Logging failed", e));
  }, [currentUser]);

  const handleLoadingChange = useCallback((vis, prog, lbl) => {
    setLoading(vis); setLoadProg(prog); setLoadLabel(lbl);
  }, []);

  const onUploaded = (err, source) => {
    if (source) setUploadStepOverride(source);
    setMappingError(err || null);
    intentToUpload.current = false;
    if (err) {
      handleLoadingChange(false, 100, '');
    } else {
      setChartsReady(false);
      setPmReady(false);
    }
    setDataLoaded(true);
    handleRefresh();
  };

  const handleSignOut = async () => {
    logAction('LOGOUT', 'User signed out');
    try { await fetch(`${API}/p2p/clear`, { method: 'POST' }); } catch (e) { }
    intentToUpload.current = true;
    setDataLoaded(false);
    setChartsReady(false);
    setPmReady(false);
    setKpis(null);
    if (onSignOut) onSignOut();
  };

  const handleFixMapping = (sourceType) => {
    logAction('FIX_MAPPING', 'Navigated to column mapping');
    intentToUpload.current = true;
    setDataLoaded(false);
    setChartsReady(false);
    setPmReady(false);
    // Navigate to the appropriate upload screen based on how data was loaded
    setUploadStepOverride(sourceType || 'table');
    setUploadStep(sourceType || 'table');
  };

  const handleResetData = () => {
    logAction('RESET_DATA', 'Started upload new file flow');
    setMappingError(null);
    intentToUpload.current = true;
    setDataLoaded(false);
    setChartsReady(false);
    setPmReady(false);
    setUploadStepOverride(null);
    setUploadStep('info');
    setKpis(null);
    setActData([]);
    setCaseTableData([]);
    setCaseEvents([]);
    setErnamData([]);
    setSelected({
      company: 'ALL', bsart: 'ALL', matkl: 'ALL', vendor: 'ALL',
      purch_group: 'ALL', case_id: 'ALL',
      month: 'ALL', quarter: 'ALL', lead_time: 'ALL', events: 'ALL', ernam: 'ALL', status: 'ALL',
      start_date: '', end_date: ''
    });
    setCrossFilter(null);
  };

  const handleRefresh = () => {
    logAction('REFRESH', 'Refreshed the dashboard');
    setRefreshTrigger(p => p + 1);
  };

  useEffect(() => {
    if (!currentUser) return;
    const ping = () => fetch(`${API}/`).then(r => r.ok ? r.json() : null)
      .then(d => {
        setServerOk(!!(d?.status));
        if (d?.data_loaded && !intentToUpload.current) {
          setDataLoaded(prev => {
            if (!prev) {
              setLoading(true);
              setLoadProg(100);
              setLoadLabel('Loading existing dashboard...');
              return true;
            }
            return prev;
          });
        }
      }).catch(() => {
        setServerOk(false);
      });
    ping(); const t = setInterval(ping, 5000); return () => clearInterval(t);
  }, [currentUser]);

  const baseQStr = useCallback(() => {
    const q = qs(selected);
    const userParam = `username=${encodeURIComponent(currentUser || 'Unknown')}`;
    return q ? `${q}&${userParam}` : `?${userParam}`;
  }, [selected, currentUser]);

  const effectiveQStr = useCallback(() => {
    let q;
    if (!crossFilter || !CROSS_TO_PARAM[crossFilter.type]) {
      q = qs(selected);
    } else {
      q = qs({ ...selected, [CROSS_TO_PARAM[crossFilter.type]]: crossFilter.value });
    }
    const userParam = `username=${encodeURIComponent(currentUser || 'Unknown')}`;
    return q ? `${q}&${userParam}` : `?${userParam}`;
  }, [crossFilter, selected, currentUser]);

  const handleSelect = useCallback((type, value) => {
    setCrossFilter(prev => {
      const isRemoving = prev?.type === type && prev?.value === value;
      if (isRemoving) logAction('FILTER', `Cleared cross-filter on chart: ${type}`);
      else logAction('FILTER', `Applied cross-filter on chart: ${type} = ${value}`);
      return isRemoving ? null : { type, value };
    });
  }, [logAction]);

  const clearCF = useCallback(() => {
    logAction('FILTER', 'Cleared all active chart cross-filters');
    setCrossFilter(null);
  }, [logAction]);

  useEffect(() => {
    if (!dataLoaded) return;
    fetch(`${API}/p2p/filters${baseQStr()}`)
      .then(r => r.ok ? r.json() : {})
      .then(d => setFilters(d && typeof d === 'object' && !Array.isArray(d) ? d : {}))
      .catch(() => setFilters({}));
  }, [baseQStr, dataLoaded, refreshTrigger]);

  useEffect(() => {
    if (!dataLoaded) return;
    fetch(`${API}/p2p/date_range?username=${encodeURIComponent(currentUser || 'Unknown')}`)
      .then(r => r.ok ? r.json() : {})
      .then(d => setDateRange(d && d.min_date ? d : { min_date: null, max_date: null }))
      .catch(() => setDateRange({ min_date: null, max_date: null }));
  }, [dataLoaded, refreshTrigger, currentUser]);

  useEffect(() => {
    if (!dataLoaded) return;
    if (selected.case_id !== 'ALL' && selected.case_id != null) {
      fetch(`${API}/p2p/case_events?case_id=${encodeURIComponent(selected.case_id)}&username=${encodeURIComponent(currentUser || 'Unknown')}`)
        .then(r => r.ok ? r.json() : [])
        .then(setCaseEvents)
        .catch(() => setCaseEvents([]));
    } else {
      setCaseEvents([]);
    }
  }, [selected.case_id, dataLoaded, refreshTrigger, currentUser]);

  useEffect(() => {
    if (!dataLoaded) return;
    setDashboardLoading(true);
    if (pmReady) setPmLoading(true);
    setPmError('');
    const startTime = Date.now();

    const cq = effectiveQStr();

    fetch(`${API}/p2p/dashboard-batch${cq}`)
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then(d => {
        setKpis(d.kpis || { total_cases: 0 });
        setActData(d.activity || []);
        setMonData(d.monthly || []);
        setCompData(d.company || []);
        setBsData(d.bsart || []);
        setMkData(d.matkl || []);
        setVendData(d.vendors || []);
        setLtData(d.leadtime || []);
        setErnamData(d.ernam || []);
        setCaseTableData(d.cases || []);

        setPoRevErnam(d.po_rev_ernam || []);
        setPoRevTimeline(d.po_rev_timeline || []);
        setPrRevAfterPo(d.pr_rev_after_po_ernam || []);
        setSeqViolation(d.seq_violation_ernam || []);
        setHappyPathData(d.happy_path || []);
        setSodData(d.sod_violations || []);
        setBottleneckData(d.bottleneck || []);
        setRevByPurchGroup(d.rev_by_purch_group || []);
        setPurchGroupWorkload(d.purch_group_workload || []);
        setVendorLeadTime(d.vendor_lead_time || []);

        if (d.process_map) {
          setRawGraphData(d.process_map);
        }
      })
      .catch(err => {
        console.error("Failed to load dashboard batch:", err);
        setKpis({ total_cases: 0 });
        setActData([]);
        setPmError(`Failed: ${err.message}`);
      })
      .finally(() => {
        const elapsed = Date.now() - startTime;
        const minDelay = 1200;
        const remaining = Math.max(0, minDelay - elapsed);
        setTimeout(() => {
          setDashboardLoading(false);
          setChartsReady(true);
          setPmLoading(false);
          setPmReady(true);
        }, remaining);
      });

  }, [effectiveQStr, dataLoaded, refreshTrigger]);

  useEffect(() => {
    if (dataLoaded && chartsReady && pmReady && loading) {
      setLoading(false);
    }
  }, [dataLoaded, chartsReady, pmReady, loading]);

  useEffect(() => {
    if (rawGraphData) {
      buildFlowMap(rawGraphData.nodes, rawGraphData.edges, setRfNodes, setRfEdges, layoutDir);
    }
  }, [layoutDir, rawGraphData]);

  const slicer = (key, label, filterKey, style = {}) => {
    const raw = filters[filterKey];
    const opts = Array.isArray(raw) ? raw : ['ALL'];
    const deduped = opts[0] === 'ALL' ? opts : ['ALL', ...opts];

    const handleSlicerChange = (val) => {
      logAction('FILTER', `Changed slicer ${key} to ${val}`);
      setSelected(prev => ({ ...prev, [key]: val }));
      setCrossFilter(null);
    };

    if (key === 'case_id' || key === 'vendor' || key === 'lifnr' || key === 'events') {
      return (
        <SearchableSelect key={key} label={label} value={selected[key] || 'ALL'}
          options={deduped} style={style}
          onChange={handleSlicerChange} />
      );
    }

    return (
      <FilterSelect key={key} label={label} value={selected[key] || 'ALL'}
        options={deduped} style={style}
        onChange={handleSlicerChange} />
    );
  };

  const resetAll = () => {
    logAction('FILTER', 'Reset all slicers to ALL');
    setSelected({
      company: 'ALL', bsart: 'ALL', matkl: 'ALL', vendor: 'ALL',
      purch_group: 'ALL', case_id: 'ALL',
      month: 'ALL', quarter: 'ALL', lead_time: 'ALL', events: 'ALL', ernam: 'ALL', status: 'ALL',
      start_date: '', end_date: ''
    });
    setCrossFilter(null);
  };

  const kpiTooltips = {
    total_cases: 'Total number of unique purchase processes',
    po_created: 'Cases with a Purchase Order created',
    gr_postings: 'Cases with a Goods Receipt posted',
    invoices_posted: 'Cases with an Invoice posted',
    reversals: 'Total reversal events across all cases',
    unique_vendors: 'Distinct vendors in the dataset',
    po_without_pr: 'POs raised without a preceding PR',
    pr_rev_after_po: 'PR reversals that happened after PO creation',
    po_rev_after_gr: 'PO reversals that happened after GR',
    gr_no_invoice: 'Goods Receipts without a corresponding Invoice',
    inv_no_gr: 'Invoices posted before Goods Receipt',
  };

  return (
    <div style={{
      fontFamily: "'Segoe UI',-apple-system,sans-serif",
      background: C.bg, height: '100vh', display: 'flex', flexDirection: 'column', overflow: 'hidden'
    }}>
      <div ref={screenshotRef} style={{ display: 'flex', flexDirection: 'column', flex: 1, overflowY: 'auto' }}>

        <style>{`
        @keyframes spin{to{transform:rotate(360deg)}}
        *{box-sizing:border-box}
        ::-webkit-scrollbar{width:5px;height:5px}
        ::-webkit-scrollbar-track{background:transparent}
        ::-webkit-scrollbar-thumb{background:#D2D0CE;border-radius:3px}
        ::-webkit-scrollbar-thumb:hover{background:#A19F9D}
      `}</style>

        <LoadingOverlay visible={loading} progress={loadProg} label={loadLabel} />

        <div style={{
          background: C.headerBg, padding: '10px 20px', flexShrink: 0,
          display: 'flex', justifyContent: 'space-between', alignItems: 'center',
          boxShadow: '0 2px 8px rgba(0,0,0,.2)'
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
            <img
              src="/logo.png"
              alt="AJALabs Logo"
              onClick={() => onBackHome && onBackHome()}
              title="Back to Home"
              style={{ height: '36px', objectFit: 'contain', cursor: 'pointer', borderRadius: 4, transition: 'opacity 0.2s' }}
              onMouseOver={e => { e.currentTarget.style.opacity = '0.7'; }}
              onMouseOut={e => { e.currentTarget.style.opacity = '1'; }}
            />
            <div>
              <div style={{ fontWeight: 700, fontSize: 16, color: '#fff' }}>P2P Process Explorer</div>
              <div style={{ fontSize: 10, color: 'rgba(255,255,255,.5)' }}>Procure-to-Pay Process Mining</div>
            </div>
            {crossFilter && (
              <div style={{
                display: 'flex', alignItems: 'center', gap: 6, marginLeft: 16,
                background: 'rgba(255,255,255,.12)', border: '1px solid rgba(255,255,255,.2)',
                borderRadius: 6, padding: '4px 12px', fontSize: 12
              }}>
                <span style={{ color: '#fff', fontWeight: 600 }}>Filter: {crossFilter.type}: <strong>{crossFilter.value}</strong></span>
                <button onClick={clearCF} style={{
                  background: 'none', border: 'none', cursor: 'pointer',
                  color: 'rgba(255,255,255,.8)', fontWeight: 700, fontSize: 14, padding: '0 2px'
                }}>X</button>
              </div>
            )}
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
            {dataLoaded && kpis && (
              <div style={{ fontSize: 11, color: 'rgba(255,255,255,.5)' }}>
                {Number(kpis.total_cases).toLocaleString()} cases loaded
              </div>
            )}

            {dataLoaded && (
              <div style={{
                display: 'flex', alignItems: 'center', gap: 4,
                background: 'rgba(255,255,255,0.08)', borderRadius: '24px',
                padding: '4px', border: '1px solid rgba(255,255,255,0.1)'
              }}>
                <button
                  style={getTabStyle(activeTab === 'process')}
                  onClick={() => {
                    if (activeTab !== 'process') {
                      logAction('TAB', 'Viewed Process Mining');
                      setActiveTab('process');
                      setTabSkeleton(true);
                      setTimeout(() => setTabSkeleton(false), 500);
                    }
                  }}
                >
                  Process Mining
                </button>
                <button
                  style={getTabStyle(activeTab === 'dimensions')}
                  onClick={() => {
                    if (activeTab !== 'dimensions') {
                      logAction('TAB', 'Viewed Dimensions');
                      setActiveTab('dimensions');
                      setTabSkeleton(true);
                      setTimeout(() => setTabSkeleton(false), 500);
                    }
                  }}
                >
                  EDA
                </button>
                <button
                  style={{ ...getTabStyle(false), color: '#A8D4FF' }}
                  onClick={handleResetData}
                  onMouseOver={e => { e.currentTarget.style.background = 'rgba(255,255,255,0.1)'; }}
                  onMouseOut={e => { e.currentTarget.style.background = 'transparent'; }}
                >
                  Upload New File
                </button>
              </div>
            )}

            {dataLoaded && !loading && !tabSkeleton && !pmLoading && (
              <button
                id="p2p-screenshot-btn"
                onClick={handleScreenshot}
                title="Download full report screenshot"
                style={{
                  width: 36, height: 36, borderRadius: '50%',
                  background: screenshotting
                    ? 'rgba(0,120,212,0.5)'
                    : 'linear-gradient(135deg, rgba(0,120,212,0.9) 0%, rgba(0,80,160,0.95) 100%)',
                  border: '1.5px solid rgba(255,255,255,0.25)',
                  boxShadow: screenshotting
                    ? '0 0 0 3px rgba(0,120,212,0.4)'
                    : '0 2px 12px rgba(0,120,212,0.45), 0 0 0 1px rgba(255,255,255,0.08)',
                  cursor: screenshotting ? 'not-allowed' : 'pointer',
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                  flexShrink: 0, transition: 'all 0.22s ease',
                  marginLeft: 8,
                }}
                onMouseOver={e => { if (!screenshotting) { e.currentTarget.style.transform = 'scale(1.12)'; e.currentTarget.style.boxShadow = '0 4px 18px rgba(0,120,212,0.7), 0 0 0 1px rgba(255,255,255,0.15)'; } }}
                onMouseOut={e => { e.currentTarget.style.transform = 'scale(1)'; e.currentTarget.style.boxShadow = '0 2px 12px rgba(0,120,212,0.45), 0 0 0 1px rgba(255,255,255,0.08)'; }}
              >
                {screenshotting ? (
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" style={{ animation: 'spin 1s linear infinite' }}>
                    <path d="M21 12a9 9 0 11-6.219-8.56" />
                  </svg>
                ) : (
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M23 19a2 2 0 01-2 2H3a2 2 0 01-2-2V8a2 2 0 012-2h4l2-3h6l2 3h4a2 2 0 012 2z" />
                    <circle cx="12" cy="13" r="4" />
                  </svg>
                )}
              </button>
            )}

            <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginLeft: 16 }}>
              <div style={{ width: 1, height: 24, background: 'rgba(255,255,255,0.2)' }}></div>
              <div style={{ fontSize: 12, color: 'rgba(255,255,255,0.7)' }}>
                User: <strong style={{ color: '#fff' }}>{currentUser}</strong>
              </div>
              <button onClick={handleSignOut} style={{
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
        </div>

        <div style={{
          flex: 1, overflowY: 'auto', padding: '12px 14px 40px',
          display: 'flex', flexDirection: 'column', gap: 10
        }}>

          {!dataLoaded && (
            <UploadBanner
              currentUser={currentUser}
              onUploaded={onUploaded}
              serverOk={serverOk}
              onLoadingChange={handleLoadingChange}
              myFiles={myFiles}
              fetchingFiles={fetchingFiles}
              handleLoadOldFile={handleLoadOldFile}
              step={uploadStep}
              setStep={setUploadStep}
              introStep={introStep}
              setIntroStep={setIntroStep} />
          )}

          {dataLoaded && (mappingError || (kpis && kpis.total_cases === 0 && !hasActiveFilters)) && (
            <div style={{
              background: '#FFF4CE', border: '1px solid #FDE7E9', borderRadius: 8, padding: '12px 20px',
              display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 16,
              boxShadow: '0 2px 4px rgba(0,0,0,0.05)', marginBottom: 2
            }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                <span style={{ fontSize: 20 }}>⚠️</span>
                <div>
                  <div style={{ fontWeight: 700, fontSize: 13, color: '#323130' }}>Column mapping might be wrong</div>
                  <div style={{ fontSize: 12, color: '#605E5C', marginTop: 2 }}>{mappingError || "No cases found. Please check your column mappings."}</div>
                </div>
              </div>
              <button onClick={() => handleFixMapping(uploadStepOverride || 'table')}
                style={{ padding: '8px 20px', background: '#0078D4', color: '#fff', border: 'none', borderRadius: 6, fontWeight: 700, cursor: 'pointer', fontSize: 12, transition: 'all 0.2s' }}
                onMouseOver={e => e.currentTarget.style.background = '#005A9E'}
                onMouseOut={e => e.currentTarget.style.background = '#0078D4'}>
                Fix Mapping
              </button>
            </div>
          )}
          {dataLoaded && (<>
            <div style={{
              background: C.card, borderRadius: 8, padding: '10px 14px',
              border: `1px solid ${C.border}`, boxShadow: '0 2px 6px rgba(0,0,0,.04)'
            }}>

              <div style={{
                display: 'flex',
                flexWrap: 'wrap',
                gap: '12px',
                alignItems: 'end'
              }}>
                {slicer('case_id', 'Case ID', 'case_ids', { flex: '1.5 1 180px' })}
                {slicer('company', 'Company', 'companies', { flex: '0.4 1 80px', minWidth: '80px' })}
                {slicer('matkl', 'Material Group', 'matkls', { flex: '1 1 140px' })}
                {slicer('vendor', 'Vendor', 'vendors', { flex: '1.5 1 180px' })}
                {slicer('purch_group', 'Purchasing Group', 'purch_groups', { flex: '1 1 140px' })}
                <FilterSelect
                  key="lead_time"
                  label="Lead Time"
                  value={selected.lead_time || 'ALL'}
                  options={["ALL", "0-5 Days", "5-10 Days", "10-20 Days", "20-50 Days", "50+ Days"]}
                  style={{ flex: '1 1 140px' }}
                  onChange={(val) => {
                    logAction('FILTER', `Changed slicer lead_time to ${val}`);
                    setSelected(prev => ({ ...prev, lead_time: val }));
                    setCrossFilter(null);
                  }}
                />
                {slicer('events', 'Events', 'events', { flex: '1 1 140px' })}
                <DateRangeFilter
                  label="Date Range"
                  fromValue={selected.start_date}
                  toValue={selected.end_date}
                  minDate={dateRange.min_date}
                  maxDate={dateRange.max_date}
                  onFromChange={(val) => {
                    logAction('FILTER', `Changed start_date to ${val}`);
                    setSelected(prev => ({ ...prev, start_date: val }));
                  }}
                  onToChange={(val) => {
                    logAction('FILTER', `Changed end_date to ${val}`);
                    setSelected(prev => ({ ...prev, end_date: val }));
                  }}
                />

                <div style={{ display: 'flex', flexDirection: 'column', gap: 4, justifyContent: 'flex-end' }}>
                  <div style={{ fontSize: 11, color: 'transparent', fontWeight: 600, userSelect: 'none' }}>.</div>
                  <div style={{ display: 'flex', gap: 4, alignItems: 'center' }}>
                    {/* Reset Button – blue P2P theme */}
                    <button
                      onClick={resetAll}
                      title="Reset all filters"
                      style={{
                        display: 'flex', alignItems: 'center', justifyContent: 'center',
                        width: 28, height: 26, padding: 0,
                        background: 'linear-gradient(135deg, #EFF6FF 0%, #DBEAFE 100%)',
                        border: '1px solid #93C5FD', borderRadius: 5, cursor: 'pointer',
                        boxShadow: '0 1px 3px rgba(0,120,212,0.15)', transition: 'all 0.2s',
                        flexShrink: 0,
                      }}
                      onMouseOver={e => { e.currentTarget.style.background = 'linear-gradient(135deg,#DBEAFE 0%,#BFDBFE 100%)'; e.currentTarget.style.boxShadow = '0 2px 6px rgba(0,120,212,0.25)'; }}
                      onMouseOut={e => { e.currentTarget.style.background = 'linear-gradient(135deg,#EFF6FF 0%,#DBEAFE 100%)'; e.currentTarget.style.boxShadow = '0 1px 3px rgba(0,120,212,0.15)'; }}
                    >
                      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#0078D4" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round">
                        <path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8" />
                        <path d="M3 3v5h5" />
                      </svg>
                    </button>
                    {/* Refresh Button – blue P2P theme */}
                    <button
                      onClick={handleRefresh}
                      title="Refresh data"
                      style={{
                        display: 'flex', alignItems: 'center', justifyContent: 'center',
                        width: 28, height: 26, padding: 0,
                        background: 'linear-gradient(135deg, #0078D4 0%, #005A9E 100%)',
                        border: 'none', borderRadius: 5, cursor: 'pointer',
                        boxShadow: '0 1px 3px rgba(0,120,212,0.35)', transition: 'all 0.2s',
                        flexShrink: 0,
                      }}
                      onMouseOver={e => { e.currentTarget.style.background = 'linear-gradient(135deg,#005A9E 0%,#004578 100%)'; e.currentTarget.style.boxShadow = '0 2px 6px rgba(0,120,212,0.5)'; }}
                      onMouseOut={e => { e.currentTarget.style.background = 'linear-gradient(135deg,#0078D4 0%,#005A9E 100%)'; e.currentTarget.style.boxShadow = '0 1px 3px rgba(0,120,212,0.35)'; }}
                    >
                      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#ffffff" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round">
                        <polyline points="23 4 23 10 17 10" />
                        <polyline points="1 20 1 14 7 14" />
                        <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15" />
                      </svg>
                    </button>
                    {/* Download CSV Button */}
                    <button
                      onClick={() => {
                        const url = `${API}/p2p/download_output?username=${encodeURIComponent(currentUser || 'Unknown')}`;
                        const a = document.createElement('a');
                        a.href = url; a.download = ''; a.click();
                        logAction('DOWNLOAD', 'Downloaded output CSV from dashboard');
                      }}
                      title="Download current dataset as CSV"
                      style={{
                        display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 4,
                        padding: '0 8px', height: 26,
                        background: 'linear-gradient(135deg, #005A9E 0%, #003E6B 100%)',
                        color: '#fff', border: 'none', borderRadius: 5, cursor: 'pointer',
                        fontSize: 11, fontWeight: 700, whiteSpace: 'nowrap',
                        boxShadow: '0 1px 3px rgba(0,90,158,0.35)', transition: 'all 0.2s',
                        flexShrink: 0,
                      }}
                      onMouseOver={e => { e.currentTarget.style.background = 'linear-gradient(135deg,#003E6B 0%,#002D50 100%)'; e.currentTarget.style.boxShadow = '0 2px 6px rgba(0,90,158,0.5)'; }}
                      onMouseOut={e => { e.currentTarget.style.background = 'linear-gradient(135deg,#005A9E 0%,#003E6B 100%)'; e.currentTarget.style.boxShadow = '0 1px 3px rgba(0,90,158,0.35)'; }}
                    >
                      <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                        <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
                        <polyline points="7 10 12 15 17 10" />
                        <line x1="12" y1="15" x2="12" y2="3" />
                      </svg>
                      CSV
                    </button>
                  </div>
                </div>
              </div>
            </div>

            <AnimatePresence mode="wait">
              {kpis && (
                <motion.div
                  key={activeTab + "-kpis"}
                  initial="hidden" animate="visible" exit="exit"
                  variants={{
                    hidden: { opacity: 0 },
                    visible: { opacity: 1, transition: { duration: 0.4 } },
                    exit: { opacity: 0, transition: { duration: 0.4 } }
                  }}
                >
                  <EmptyState condition={kpis.total_cases === 0} message="No valid cases found. The column mapping may be incorrect. Please check your mapping and rebuild the event log.">
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(7,1fr)', gap: 8 }}>
                      <div>
                        <KpiCard label="Cases" value={kpis.total_cases} color="#6a3382" tooltip={kpiTooltips.total_cases} />
                      </div>
                      <div>
                        <KpiCard label="PO Created" value={kpis.po_created} color="#6a3382" tooltip={kpiTooltips.po_created} />
                      </div>
                      <div>
                        <KpiCard label="GR Postings" value={kpis.gr_postings} color="#6a3382" tooltip={kpiTooltips.gr_postings} />
                      </div>
                      <div>
                        <KpiCard label="Invoices Posted" value={kpis.invoices_posted} color="#6a3382" tooltip={kpiTooltips.invoices_posted} />
                      </div>
                      <div>
                        <KpiCard label="Total Reversals" value={kpis.reversals} color="#6a3382" tooltip={kpiTooltips.reversals} />
                      </div>
                      <div>
                        <KpiCard label="Vendors" value={kpis.unique_vendors} color="#6a3382" tooltip={kpiTooltips.unique_vendors} />
                      </div>
                      <div>
                        <KpiCard label="Avg Completion (Days)" value={kpis.avg_completion_days} color="#6a3382" tooltip="Average end-to-end process duration in days" />
                      </div>
                    </div>

                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5,1fr)', gap: 8, marginTop: 8 }}>
                      <div>
                        <ConfKpiCard label="PO Without PR" value={kpis.po_without_pr} color="#6a3382" sub="PO raised without PR" tooltip={kpiTooltips.po_without_pr} />
                      </div>
                      <div>
                        <ConfKpiCard label="PR Rev. After PO Date" value={kpis.pr_rev_after_po} color="#6a3382" sub="Late PR reversals" tooltip={kpiTooltips.pr_rev_after_po} />
                      </div>
                      <div>
                        <ConfKpiCard label="PO Rev. After GR" value={kpis.po_rev_after_gr} color="#6a3382" sub="Late PO reversals" tooltip={kpiTooltips.po_rev_after_gr} />
                      </div>
                      <div>
                        <ConfKpiCard label="GR Without Invoice" value={kpis.gr_no_invoice} color="#6a3382" sub="GR not yet invoiced" tooltip={kpiTooltips.gr_no_invoice} />
                      </div>
                      <div>
                        <ConfKpiCard label="Invoice Without GR" value={kpis.inv_no_gr} color="#6a3382" sub="Invoice before GR" tooltip={kpiTooltips.inv_no_gr} />
                      </div>
                    </div>
                  </EmptyState>
                </motion.div>
              )}
            </AnimatePresence>

            {activeTab === 'process' ? (
              <div
                key="process"
                style={{ display: 'flex', flexDirection: 'column', gap: 10, flex: 1, paddingBottom: '20px' }}
              >
                <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 10 }}>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>

                    <div style={{
                      background: C.card, borderRadius: 8, border: `1px solid ${C.border}`,
                      boxShadow: '0 2px 8px rgba(0,0,0,.05)', overflow: 'hidden',
                      display: 'flex', flexDirection: 'column', height: 915
                    }}>

                      <div style={{
                        padding: '12px 14px 8px', borderBottom: `1px solid ${C.border}`,
                        background: C.jkBlue,
                        display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexShrink: 0
                      }}>
                        <div>
                          <div style={{ fontSize: 13, fontWeight: 700, color: '#fff' }}>Process Map</div>
                          <div style={{ fontSize: 10, color: 'rgba(255,255,255,0.7)' }}>Flow & Frequency Analysis</div>
                        </div>
                        <div style={{ display: 'flex', gap: 4, background: 'rgba(255,255,255,0.2)', padding: 2, borderRadius: 4 }}>
                          <button
                            onClick={() => setLayoutDir('LR')}
                            style={{
                              fontSize: 11, padding: '4px 8px', border: 'none', cursor: 'pointer', borderRadius: 3,
                              background: layoutDir === 'LR' ? '#fff' : 'transparent',
                              color: layoutDir === 'LR' ? C.jkBlue : '#fff',
                              fontWeight: layoutDir === 'LR' ? 700 : 400
                            }}>Horizontal</button>
                          <button
                            onClick={() => setLayoutDir('TB')}
                            style={{
                              fontSize: 11, padding: '4px 8px', border: 'none', cursor: 'pointer', borderRadius: 3,
                              background: layoutDir === 'TB' ? '#fff' : 'transparent',
                              color: layoutDir === 'TB' ? C.jkBlue : '#fff',
                              fontWeight: layoutDir === 'TB' ? 700 : 400
                            }}>Vertical</button>
                        </div>
                      </div>

                      <div style={{ flex: 1, position: 'relative' }}>
                        {pmError && (
                          <div style={{
                            position: 'absolute', top: 8, left: 8, right: 8, zIndex: 10,
                            fontSize: 11, color: '#A4262C', background: '#FDE7E9',
                            border: '1px solid #FBC5C9', borderRadius: 4, padding: '6px 12px'
                          }}>
                            Error: {pmError}
                          </div>
                        )}

                        {(pmLoading || tabSkeleton) && (
                          <div style={{
                            position: 'absolute', inset: 0, zIndex: 20,
                            background: 'rgba(248,250,255,0.88)', backdropFilter: 'blur(4px)',
                            display: 'flex', flexDirection: 'column',
                            alignItems: 'center', justifyContent: 'center', gap: 14, borderRadius: 6
                          }}>
                            <div style={{ textAlign: 'center' }}>
                              <div style={{ fontSize: 13, fontWeight: 700, color: '#323130', marginBottom: 4 }}>
                                Building Process Map
                              </div>
                              <div style={{ fontSize: 11, color: C.slate }}>
                                Analysing transitions and paths…
                              </div>
                            </div>
                          </div>
                        )}

                        <div style={{ position: 'absolute', inset: 0, background: '#FAFAFA' }}>
                          <ReactFlow
                            nodes={rfNodes} edges={rfEdges}
                            onNodesChange={onNodesChange} onEdgesChange={onEdgesChange}
                            nodeTypes={nodeTypes} edgeTypes={edgeTypes}
                            fitView fitViewOptions={{ padding: .18 }}
                            minZoom={0.1} maxZoom={4}
                            proOptions={{ hideAttribution: true }}
                            onNodeMouseEnter={(e, n) => setHoverInfo({
                              x: e.clientX, y: e.clientY,
                              title: n.data?.label || '', value: n.data?.frequency || 0
                            })}
                            onNodeMouseLeave={() => setHoverInfo(null)}
                            onEdgeMouseEnter={(e, ed) => setHoverInfo({
                              x: e.clientX, y: e.clientY,
                              title: `${ed.source} → ${ed.target}`,
                              value: ed.data?.frequency || 0, isEdge: true, avgDays: ed.data?.avg_days
                            })}
                            onEdgeMouseLeave={() => setHoverInfo(null)}
                            defaultEdgeOptions={{ type: 'freqEdge' }}>
                            <Background color="#C8D3E8" gap={24} size={1.5} variant="dots" />
                            <Controls showInteractive={false}
                              style={{ background: '#fff', border: `1px solid ${C.border}`, borderRadius: 6 }} />
                            <MiniMap zoomable pannable
                              nodeColor={C.mapNodeBg}
                              maskColor="rgba(240,244,250,.85)"
                              style={{ border: `1px solid ${C.border}`, borderRadius: 6 }} />
                          </ReactFlow>
                        </div>

                        {hoverInfo && (
                          <div style={{
                            position: 'fixed', left: hoverInfo.x + 16, top: hoverInfo.y + 16,
                            zIndex: 99999, pointerEvents: 'none',
                            background: 'rgba(255,255,255,.98)', border: `1px solid ${C.border}`,
                            borderRadius: 6, padding: '10px 14px',
                            boxShadow: '0 4px 12px rgba(0,0,0,.15)', fontSize: 12, color: '#323130', minWidth: 160
                          }}>
                            <div style={{ fontWeight: 700, color: '#0078D4', marginBottom: 6 }}>{hoverInfo.title}</div>
                            <div style={{ display: 'flex', justifyContent: 'space-between', gap: 16 }}>
                              <span style={{ color: C.slate }}>{hoverInfo.isEdge ? 'Transitions:' : 'Unique Cases:'}</span>
                              <strong>{Number(hoverInfo.value).toLocaleString()}</strong>
                            </div>
                            {hoverInfo.avgDays != null && (
                              <div style={{ display: 'flex', justifyContent: 'space-between', gap: 16, marginTop: 4 }}>
                                <span style={{ color: C.slate }}>Avg Duration:</span>
                                <strong>{hoverInfo.avgDays}d</strong>
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                    </div>
                  </div>

                  <div style={{ display: 'flex', flexDirection: 'column', gap: 10, height: '100%' }}>

                    <ChartCard title="Happy Path vs Deviations" loading={dashboardLoading || tabSkeleton} highlighted={crossFilter?.type === 'status'} onClear={clearCF} skeletonType="pie">
                      <StatusDonutChart data={happyPathData} crossFilter={crossFilter} onSelect={handleSelect} isAnimationActive={false} />
                    </ChartCard>

                    <ChartCard title="Activity Frequency"
                      loading={dashboardLoading || tabSkeleton}
                      highlighted={crossFilter?.type === 'activity'} onClear={clearCF} skeletonType="bar-horizontal">
                      <ActivityChart data={actData} crossFilter={crossFilter} onSelect={handleSelect} isAnimationActive={false} />
                    </ChartCard>

                    <ChartCard title="Lead Time Distribution"
                      loading={dashboardLoading || tabSkeleton}
                      highlighted={crossFilter?.type === 'lead_time'} onClear={clearCF} skeletonType="bar-vertical">
                      <LeadTimeChart data={ltData} crossFilter={crossFilter} onSelect={handleSelect} isAnimationActive={false} />
                    </ChartCard>

                  </div>
                </div>

                <div style={{ marginTop: '2px' }}>
                  <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>

                    <ChartCard title="SoD Violations" subtitle="Cases with segregation of duties violations" loading={dashboardLoading || tabSkeleton} highlighted={crossFilter?.type === 'sod'} onClear={clearCF} skeletonType="bar-vertical">
                      <EmptyState condition={!dashboardLoading && sodData.length === 0} message="No SoD violations found.">
                        <SodViolationsChart data={sodData} crossFilter={crossFilter} onSelect={handleSelect} />
                      </EmptyState>
                    </ChartCard>

                    <ChartCard title="PO Reversals Timeline" subtitle="Trend of PO reversals over time" loading={dashboardLoading || tabSkeleton} highlighted={crossFilter?.type === 'month'} onClear={clearCF} skeletonType="bar-vertical">
                      <EmptyState condition={!dashboardLoading && poRevTimeline.length === 0} message="No PO reversals found.">
                        <MonthlyChart data={poRevTimeline} crossFilter={crossFilter} onSelect={handleSelect} isAnimationActive={false} />
                      </EmptyState>
                    </ChartCard>

                    <ChartCard title="PO Reversals by Purchasing Group" subtitle="Purchasing groups with the most PO reversal activity — click a bar to filter" loading={dashboardLoading || tabSkeleton} highlighted={crossFilter?.type === 'purch_group'} onClear={clearCF} skeletonType="bar-vertical">
                      <EmptyState condition={!dashboardLoading && revByPurchGroup.length === 0} message="No PO reversals found.">
                        <RevByPurchGroupVBarChart data={revByPurchGroup} crossFilter={crossFilter} onSelect={handleSelect} />
                      </EmptyState>
                    </ChartCard>

                    <ChartCard title="PO Reversals by Username" subtitle="Users reversing the most POs" loading={dashboardLoading || tabSkeleton} highlighted={crossFilter?.type === 'ernam'} onClear={clearCF} skeletonType="bar-horizontal">
                      <EmptyState condition={!dashboardLoading && poRevErnam.length === 0} message="No PO reversals found.">
                        <ScrollableHBarChart data={poRevErnam} dataKey="count" labelKey="ernam" color="#5aabee" crossFilter={crossFilter} crossKey="ernam" isAnimationActive={false} onSelect={handleSelect} />
                      </EmptyState>
                    </ChartCard>

                    <ChartCard title="Late PR Reversals (By Username)" subtitle="PRs reversed AFTER PO creation" loading={dashboardLoading || tabSkeleton} highlighted={crossFilter?.type === 'ernam'} onClear={clearCF} skeletonType="bar-horizontal">
                      <EmptyState condition={!dashboardLoading && prRevAfterPo.length === 0} message="No late PR reversals found (or Requisition data is not uploaded).">
                        <ScrollableHBarChart data={prRevAfterPo} dataKey="count" labelKey="ernam" color="#CA5010" crossFilter={crossFilter} crossKey="ernam" isAnimationActive={false} onSelect={handleSelect} />
                      </EmptyState>
                    </ChartCard>

                    <ChartCard title="PO created AFTER GR or Invoice (By Username)" loading={dashboardLoading || tabSkeleton} highlighted={crossFilter?.type === 'ernam'} onClear={clearCF} skeletonType="bar-horizontal">
                      <EmptyState condition={!dashboardLoading && seqViolation.length === 0} message="No sequence violations found (or GR/Invoice data is not uploaded).">
                        <ScrollableHBarChart data={seqViolation} dataKey="count" labelKey="ernam" color="#1aca60" crossFilter={crossFilter} crossKey="ernam" isAnimationActive={false} onSelect={handleSelect} />
                      </EmptyState>
                    </ChartCard>

                  </div>
                </div>

                <div style={{ display: 'grid', gridTemplateColumns: '1fr', gap: 10, marginTop: '10px' }}>
                  <ChartCard title="Case Details" subtitle="Click a Case ID to view its chronological event log" loading={dashboardLoading || tabSkeleton} skeletonType="timeline">
                    <CaseTable
                      data={caseTableData}
                      events={caseEvents}
                      selectedId={selected.case_id}
                      onSelect={(id) => {
                        const newId = selected.case_id === id ? 'ALL' : id;
                        setSelected(prev => ({ ...prev, case_id: newId }));
                        setCrossFilter(null);
                        logAction('FILTER', `Clicked case row: ${newId}`);
                      }}
                    />
                  </ChartCard>
                </div>
              </div>
            ) : (
              <div
                key="dimensions"
                style={{
                  display: 'grid', gridTemplateColumns: 'minmax(0,1fr) minmax(0,1fr)', gap: 10,
                  gridAutoRows: 'minmax(280px, auto)', alignItems: 'stretch', paddingBottom: '24px'
                }}
              >
                <div style={{ display: 'flex', flexDirection: 'column', minWidth: 0, minHeight: 0, height: '100%' }}>
                  <ChartCard title="Monthly Trend (Unique Cases)" subtitle="Unique active cases per month" skeletonType="line"
                    loading={dashboardLoading || tabSkeleton}
                    highlighted={crossFilter?.type === 'month'} onClear={clearCF}>
                    <MonthlyChart data={monData} crossFilter={crossFilter} onSelect={handleSelect} isAnimationActive={false} />
                  </ChartCard>
                </div>

                <div style={{ display: 'flex', flexDirection: 'column', minWidth: 0, minHeight: 0, height: '100%' }}>
                  <ChartCard title="User Activity (ERNAM)" subtitle="Who performed activities (Unique Cases)" skeletonType="bar-horizontal"
                    loading={dashboardLoading || tabSkeleton}
                    highlighted={crossFilter?.type === 'ernam'} onClear={clearCF}>
                    <ErnamChart data={ernamData} crossFilter={crossFilter} onSelect={handleSelect} isAnimationActive={false} />
                  </ChartCard>
                </div>

                <div style={{ display: 'flex', flexDirection: 'column', minWidth: 0, minHeight: 0, height: '100%' }}>
                  <ChartCard title="Company Distribution" subtitle="Cases per company code" skeletonType="pie"
                    loading={dashboardLoading || tabSkeleton}
                    highlighted={crossFilter?.type === 'company'} onClear={clearCF}>
                    <CompanyDonutChart data={compData} crossFilter={crossFilter} onSelect={handleSelect} isAnimationActive={false} />
                  </ChartCard>
                </div>

                <div style={{ display: 'flex', flexDirection: 'column', minWidth: 0, minHeight: 0, height: '100%' }}>
                  <ChartCard title="PO Type (BSART)" subtitle="Distribution by document type" skeletonType="bar-vertical"
                    loading={dashboardLoading || tabSkeleton}
                    highlighted={crossFilter?.type === 'bsart'} onClear={clearCF}>
                    <ScrollableVBarChart data={bsData} crossFilter={crossFilter} onSelect={handleSelect} isAnimationActive={false} />
                  </ChartCard>
                </div>

                <div style={{ display: 'flex', flexDirection: 'column', minWidth: 0, minHeight: 0, height: '100%' }}>
                  <ChartCard title="Material Group" subtitle="Purchasing activity by material category" skeletonType="bar-horizontal"
                    loading={dashboardLoading || tabSkeleton}
                    highlighted={crossFilter?.type === 'matkl'} onClear={clearCF}>
                    <ScrollableHBarChart data={mkData} dataKey="count" labelKey="matkl"
                      color="#0078D4" crossFilter={crossFilter} crossKey="matkl" onSelect={handleSelect} isAnimationActive={false} />
                  </ChartCard>
                </div>

                <div style={{ display: 'flex', flexDirection: 'column', minWidth: 0, minHeight: 0, height: '100%' }}>
                  <ChartCard title="Vendors by Cases" subtitle="Top suppliers by volume" skeletonType="bar-horizontal"
                    loading={dashboardLoading || tabSkeleton}
                    highlighted={crossFilter?.type === 'vendor'} onClear={clearCF}>
                    <ScrollableHBarChart data={vendData} dataKey="count" labelKey="vendor"
                      color="#005A9E" crossFilter={crossFilter} crossKey="vendor" onSelect={handleSelect} isAnimationActive={false} />
                  </ChartCard>
                </div>

                <div style={{ display: 'flex', flexDirection: 'column', minWidth: 0, minHeight: 0, height: '100%' }}>
                  <ChartCard title="Purchasing Group Workload (EKGRP)" subtitle="Cases handled per purchasing group — click a bar to filter" skeletonType="bar-horizontal"
                    loading={dashboardLoading || tabSkeleton}
                    highlighted={crossFilter?.type === 'purch_group'} onClear={clearCF}>
                    <EmptyState condition={!dashboardLoading && purchGroupWorkload.length === 0} message="No workload data found.">
                      <PurchGroupWorkloadChart data={purchGroupWorkload} crossFilter={crossFilter} onSelect={handleSelect} isAnimationActive={false} />
                    </EmptyState>
                  </ChartCard>
                </div>

                <div style={{ display: 'flex', flexDirection: 'column' }}>
                  <ChartCard title="Avg Days PO → GR by Vendor" skeletonType="bar-horizontal"
                    loading={dashboardLoading || tabSkeleton}
                    highlighted={crossFilter?.type === 'vendor'} onClear={clearCF}>
                    <EmptyState condition={!dashboardLoading && vendorLeadTime.length === 0} message="No GR data found to calculate lead time.">
                      <VendorAvgDaysChart data={vendorLeadTime} crossFilter={crossFilter} onSelect={handleSelect} isAnimationActive={false} />
                    </EmptyState>
                  </ChartCard>
                </div>

                <div style={{ gridColumn: '1 / -1', marginTop: '10px' }}>
                  <ChartCard title="Case Details (EDA)" subtitle="Click a Case ID to view its chronological event log" loading={dashboardLoading || tabSkeleton} skeletonType="timeline">
                    <CaseTableEda
                      data={caseTableData}
                      events={caseEvents}
                      selectedId={selected.case_id}
                      onSelect={(id) => {
                        const newId = selected.case_id === id ? 'ALL' : id;
                        setSelected(prev => ({ ...prev, case_id: newId }));
                        setCrossFilter(null);
                        logAction('FILTER', `Clicked case row (EDA): ${newId}`);
                      }}
                    />
                  </ChartCard>
                </div>

              </div>
            )}

          </>)}

          {/* --- NEW FILE HUB UI INSTEAD OF "NO DATA LOADED" --- */}
        </div>

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
                borderTop: `3px solid ${C.headerBg}`,
                borderRadius: '50%',
                animation: 'spin 1s linear infinite'
              }} />
              <div style={{ fontSize: 16, fontWeight: 700 }}>Generating Screenshot...</div>
              <div style={{ fontSize: 12, color: 'rgba(255,255,255,0.6)' }}>Please wait while we capture the dashboard.</div>
            </div>
          </div>
        )}

        <div style={{ textAlign: 'center', fontSize: '12px', color: '#605E5C', padding: '10px 0', borderTop: '1px solid #E1DFDD', flexShrink: 0, zIndex: 100 }}>
          ©2023 <a href="https://ajalabs.ai" target="_blank" rel="noopener noreferrer" style={{ color: '#323130', textDecoration: 'none', fontWeight: 'bold' }}>ajalabs.ai</a> All rights reserved - <a href="#" style={{ color: '#0078D4', textDecoration: 'none' }}>Data Privacy</a>
        </div>

      </div>
    </div>
  );
}

//comment
