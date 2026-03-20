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
import './App.css'; 

const API = 'http://localhost:8000';

const C = {
  blue700:'#006B3C', teal:'#038387', red:'#D13438', purple:'#5C2D91',
  slate:'#605E5C', bg:'#F0F2F5', card:'#FFFFFF', border:'#E1DFDD',
  orange:'#CA5010', green:'#107C10', selected:'#EFF6FF', selectedBorder:'#006B3C',
  headerBg:'#1B3A2A', mapNodeBg: '#A5D6C8', mapNodeBorder: '#4A9E88', mapEdge: '#6B9C8F',
  jkBlue: '#006B3C' 
};
const ACCENT=['#006B3C','#038387','#CA5010','#D13438','#5C2D91','#E3008C',
  '#00B7C3','#107C10','#F59E0B','#4F6BED','#E81123','#8B5CF6','#84CC16'];



/* ─── LOADING OVERLAY WITH PHASES ──────────────────────────────────────────── */
const LoadingOverlay=({visible,progress,label})=>{
  if(!visible) return null;

  let activeStep = 1;
  if (progress > 10 && progress < 100) activeStep = 2; 
  if (progress >= 100) activeStep = 3; 

  const phases = [
    { num: 1, name: 'Processing Data' },
    { num: 2, name: 'Analysing Data' },
    { num: 3, name: 'Dashboard Created' }
  ];

  return(
    <div style={{position:'fixed',inset:0,zIndex:99999,
      background:'rgba(27,58,42,0.92)',backdropFilter:'blur(8px)',
      display:'flex',flexDirection:'column',alignItems:'center',justifyContent:'center',gap:36}}>
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
      <div style={{display: 'flex', gap: 40, alignItems: 'center'}}>
        {phases.map((phase, index) => {
          const isActive = activeStep === phase.num;
          const isDone = activeStep > phase.num;
          const color = isActive || isDone ? '#00B7C3' : 'rgba(255,255,255,0.25)';
          
          return (
            <div key={phase.num} style={{display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 10, position: 'relative'}}>
              {index > 0 && (
                <div style={{
                  position: 'absolute', right: '100%', top: 16, width: 40, height: 2, 
                  background: isDone || isActive ? '#00B7C3' : 'rgba(255,255,255,0.15)',
                  marginRight: 10, transition: 'all 0.4s ease'
                }}/>
              )}
              <div style={{
                width: 32, height: 32, borderRadius: '50%',
                background: isDone ? '#00B7C3' : (isActive ? 'rgba(0,183,195,0.1)' : 'transparent'),
                border: `2px solid ${color}`,
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                color: isDone ? '#1B2A4A' : color,
                fontWeight: 'bold', fontSize: 14, zIndex: 2,
                transition: 'all 0.3s ease',
                boxShadow: isActive ? '0 0 12px rgba(0,183,195,0.4)' : 'none'
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

      <div style={{display:'flex', flexDirection:'column', alignItems:'center', gap:8, width: 400}}>
        <div style={{width:'100%',background:'rgba(255,255,255,.15)',borderRadius:8,height:6,overflow:'hidden'}}>
          <div style={{height:'100%',borderRadius:8,transition:'width .4s ease',
            background:'linear-gradient(90deg,#006B3C,#00B7C3)',
            width:`${progress}%`,boxShadow:'0 0 12px rgba(0,107,60,.6)'}}/>
        </div>
        <div style={{fontSize:12,color:'rgba(255,255,255,.6)'}}>
          {label}
        </div>
      </div>
    </div>
  );
};

/* ─── TOOLTIP ──────────────────────────────────────────────────── */
const CustomTooltip=({active,payload,nameKey,labelOverride})=>{
  if(!active||!payload?.length||!payload[0]) return null;
  const entry=payload[0].payload||{};
  const name=nameKey?(entry[nameKey]??''):'';
  const val=payload[0].value;
  const uc=entry?.unique_cases;
  return(
    <div style={{background:'rgba(255,255,255,.98)',border:`1px solid ${C.border}`,
      borderRadius:6,padding:'8px 14px',boxShadow:'0 4px 12px rgba(0,0,0,.15)',
      fontSize:12,color:'#323130',maxWidth:260,zIndex:9999}}>
      {name&&<div style={{fontWeight:600,marginBottom:4,color:'#006B3C',wordBreak:'break-word'}}>{name}</div>}
      <div style={{color:'#605E5C'}}>
        {labelOverride==='cases'?'Cases:':'Events:'}
        &nbsp;<strong style={{color:'#323130'}}>{val!=null?Number(val).toLocaleString():0}</strong>
      </div>
      {uc!=null&&labelOverride!=='cases'&&(
        <div style={{color:'#605E5C',marginTop:2}}>
          Unique Cases:&nbsp;<strong style={{color:'#038387'}}>{Number(uc).toLocaleString()}</strong>
        </div>
      )}
    </div>
  );
};

/* ─── PROCESS NODE (Pill Style with Halos) ────────────────────────────── */
const ProcessNode = React.memo(({ data }) => {
  const freq = data?.frequency || 0;
  const isHappyPath = data?.is_main;
  
  const haloStyle = isHappyPath 
    ? '0 0 0 10px rgba(0, 107, 60, 0.15), 0 0 0 20px rgba(0, 107, 60, 0.08)' 
    : 'none';

  return (
    <div style={{
      background: '#ffffff',
      border: `2px solid ${isHappyPath ? '#006B3C' : '#999999'}`,
      borderRadius: 100, 
      minWidth: 500,
      height: 200,
      padding: '6px',
      display: 'flex',
      alignItems: 'center',
      boxShadow: '0 4px 8px rgba(0,0,0,0.06)',
      fontFamily: "'Segoe UI', -apple-system, sans-serif",
      position: 'relative',
      zIndex: 10,
    }}>
      <div style={{
        width: 160,
        height: 155,
        borderRadius: '60%',
        background: isHappyPath ? '#006B3C' : '#605E5C',
        color: '#ffffff',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        fontSize: 40,
        fontWeight: 500,
        flexShrink: 0,
        boxShadow: haloStyle,
        zIndex: 2
      }}>
        {freq > 0 ? Number(freq).toLocaleString() : '0'}
      </div>

      <div style={{
        flex: 1,
        padding: '0 20px',
        fontSize: 45,
        fontWeight: 400,
        color: '#323130', 
        textAlign: 'centre',
        lineHeight: 1.1,
        wordBreak: 'break-word',
        zIndex: 2
      }}>
        {data?.label || ''}
      </div>

      {!isHappyPath && (
        <div style={{
          position: 'absolute',
          right: 28,
          bottom: 24,
          width: 20,
          height: 20,
          borderBottom: '3px solid #D13438', 
          borderRight: '3px solid #D13438',
          borderBottomRightRadius: 8
        }}/>
      )}

      <Handle type="target" id="top-t"    position={Position.Top}    style={{opacity:0}}/>
      <Handle type="source" id="top-s"    position={Position.Top}    style={{opacity:0}}/>
      <Handle type="target" id="bottom-t" position={Position.Bottom} style={{opacity:0}}/>
      <Handle type="source" id="bottom-s" position={Position.Bottom} style={{opacity:0}}/>
      <Handle type="target" id="left-t"   position={Position.Left}   style={{opacity:0}}/>
      <Handle type="source" id="left-s"   position={Position.Left}   style={{opacity:0}}/>
      <Handle type="target" id="right-t"  position={Position.Right}  style={{opacity:0}}/>
      <Handle type="source" id="right-s"  position={Position.Right}  style={{opacity:0}}/>
    </div>
  );
});

/* ─── FREQ EDGE (Variable weight lines) ────────────────────────────── */
const cubicBezierPoint = (p0, p1, p2, p3, t) => {
  const mt = 1 - t;
  return mt*mt*mt*p0 + 3*mt*mt*t*p1 + 3*mt*t*t*p2 + t*t*t*p3;
};

const FreqEdge=React.memo(({id,sourceX,sourceY,targetX,targetY,sourcePosition,targetPosition,data,markerEnd,style})=>{
  const curvature  = data?.curvature  ?? 0.5;
  const sweepSide  = data?.sweepSide;
  const sweepDist  = data?.sweepDist  ?? 120;
  
  const max = data?.maxFreq || 1;
  const freq = data?.frequency || 0;
  const ratio = freq / max;
  
  const isMainPath = ratio >= 0.1; 
  const strokeWidth = isMainPath ? 2 + ratio * 8 : 2;
  const arcColor = isMainPath ? '#605E5C' : '#A19F9D'; 
  const dashArray = isMainPath ? 'none' : '10, 10';

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

    const tx = cubicBezierPoint(sourceX, cx1, cx2, targetX, t+0.01) - labelX;
    const ty = cubicBezierPoint(sourceY, cy1, cy2, targetY, t+0.01) - labelY;
    const len = Math.sqrt(tx*tx + ty*ty) || 1;
    const perpX = -ty / len;
    const perpY =  tx / len;
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

    const clamp = (v, a, b) => Math.min(Math.max(v, Math.min(a,b)), Math.max(a,b));
    labelX = clamp(labelX, sourceX, targetX);
    labelY = clamp(labelY, sourceY, targetY);

    const dx = targetX - sourceX;
    const dy = targetY - sourceY;
    const len = Math.sqrt(dx*dx + dy*dy) || 1;
    labelX += (-dy / len) * 14;
    labelY += ( dx / len) * 14;
  }

  return(
    <>
      <BaseEdge id={id} path={edgePath} 
        markerEnd={{...markerEnd, color: arcColor}}
        style={{
          ...style, 
          stroke: arcColor, 
          strokeWidth: strokeWidth, 
          strokeDasharray: dashArray,
          opacity: isMainPath ? 0.9 : 0.6 
        }}
      />
      {freq > 0 && (
        <EdgeLabelRenderer>
          <div style={{
            position:'absolute',
            transform:`translate(-50%,-50%) translate(${labelX}px,${labelY}px)`,
            pointerEvents:'all',zIndex:100,
            display:'flex',flexDirection:'column',alignItems:'center',gap:2
          }}>
            <div style={{
              fontSize: 36,
              fontWeight: 400,
              color: isMainPath ? '#006B3C' : '#8A8886', 
              background:'rgba(255,255,255,0.85)',
              padding:'2px 8px',
              borderRadius:12
            }}>
              {Number(freq).toLocaleString()}
            </div>
            {data?.avg_days != null && (
              <div style={{
                fontSize:18,
                color:'#8A8886',
                background:'rgba(255,255,255,.8)',
                padding:'0 4px',
                borderRadius:2
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

const nodeTypes={processNode:ProcessNode};
const edgeTypes={freqEdge:FreqEdge};

/* ─── O2C HAPPY PATH & DEVIATION ROUTING ──────────────────────────── */
const HAPPY_PATH = [
  "SO Created", "SO Approved", "Delivery Created", "Delivery Posted",
  "Goods Issued", "Invoice Created", "Invoice Posted", "Invoice Cleared"
];
const HAPPY_IDX = Object.fromEntries(HAPPY_PATH.map((n,i)=>[n,i]));

const SIDE_ABOVE_LR = new Set(["SO Reversed", "SO Reversed After GI", "Invoice Reversed"]);
const SIDE_BELOW_LR = new Set(["GI Reversed", "Credit Memo", "Debit Memo", "Delivery Returned"]);
const SIDE_LEFT_TB  = new Set(["SO Reversed", "SO Reversed After GI", "Invoice Reversed"]);
const SIDE_RIGHT_TB = new Set(["GI Reversed", "Credit Memo", "Debit Memo", "Delivery Returned"]);

const classifyEdge = (src, tgt, sPos, tPos, dir) => {
  const sIdx = HAPPY_IDX[src];
  const tIdx = HAPPY_IDX[tgt];
  const sIsHappy = sIdx !== undefined;
  const tIsHappy = tIdx !== undefined;

  if (sIsHappy && tIsHappy) {
    const steps = tIdx - sIdx;
    if (steps === 1) {
      if (dir === 'LR') return { sh:'right-s', th:'left-t',  curvature:0.1 };
      else              return { sh:'bottom-s', th:'top-t',  curvature:0.1 };
    }
    if (steps > 1) {
      const sweepDist = 150 + steps * 120;
      if (dir === 'LR') return { sh:'top-s',   th:'top-t',   sweepSide:'top',   sweepDist };
      else              return { sh:'right-s',  th:'right-t', sweepSide:'right', sweepDist };
    }
    if (steps < 0) {
      const sweepDist = 150 + Math.abs(steps) * 120;
      if (dir === 'LR') return { sh:'bottom-s', th:'bottom-t', sweepSide:'bottom', sweepDist };
      else              return { sh:'left-s',   th:'left-t',   sweepSide:'left',   sweepDist };
    }
  }

  if (dir === 'LR') {
    const srcAbove = SIDE_ABOVE_LR.has(src);
    const tgtAbove = SIDE_ABOVE_LR.has(tgt);
    const srcBelow = SIDE_BELOW_LR.has(src);
    const tgtBelow = SIDE_BELOW_LR.has(tgt);

    if (sIsHappy && tgtAbove) return { sh:'top-s',    th:'bottom-t', curvature:0.5 };
    if (srcAbove && tIsHappy) return { sh:'bottom-s', th:'top-t',    curvature:0.5 };
    if (sIsHappy && tgtBelow) return { sh:'bottom-s', th:'top-t',    curvature:0.5 };
    if (srcBelow && tIsHappy) return { sh:'top-s',    th:'bottom-t', curvature:0.5 };
    if ((srcAbove||srcBelow) && (tgtAbove||tgtBelow))
      return { sh:'right-s', th:'left-t', curvature:0.4 };
  } else {
    const srcLeft  = SIDE_LEFT_TB.has(src);
    const tgtLeft  = SIDE_LEFT_TB.has(tgt);
    const srcRight = SIDE_RIGHT_TB.has(src);
    const tgtRight = SIDE_RIGHT_TB.has(tgt);

    if (sIsHappy && tgtLeft)  return { sh:'left-s',  th:'right-t', curvature:0.5 };
    if (srcLeft  && tIsHappy) return { sh:'right-s', th:'left-t',  curvature:0.5 };
    if (sIsHappy && tgtRight) return { sh:'right-s', th:'left-t',  curvature:0.5 };
    if (srcRight && tIsHappy) return { sh:'left-s',  th:'right-t', curvature:0.5 };
    if ((srcLeft||srcRight) && (tgtLeft||tgtRight))
      return { sh:'bottom-s', th:'top-t', curvature:0.4 };
    if (srcLeft && tIsHappy)
      return { sh:'right-s', th:'right-t', sweepSide:'right', sweepDist:160 };
  }

  const dx = tPos.x - sPos.x;
  const dy = tPos.y - sPos.y;
  if (dir === 'LR') {
    if (Math.abs(dy) < 80) {
      if (dx > 0) return { sh:'right-s',  th:'left-t',    curvature:0.3 };
      else        return { sh:'bottom-s', th:'bottom-t',  curvature:0.5 };
    }
    if (dy > 0) return { sh:'bottom-s', th:'top-t',    curvature:0.4 };
    else        return { sh:'top-s',    th:'bottom-t', curvature:0.4 };
  } else {
    if (Math.abs(dx) < 80) {
      if (dy > 0) return { sh:'bottom-s', th:'top-t',   curvature:0.3 };
      else        return { sh:'right-s',  th:'right-t', curvature:0.5 };
    }
    if (dx > 0) return { sh:'right-s', th:'left-t',  curvature:0.4 };
    else        return { sh:'left-s',  th:'right-t', curvature:0.4 };
  }
};

const buildFlowMap=(bNodes, bEdges, setRfNodes, setRfEdges, dir)=>{
  const mxF=Math.max(1,...(bNodes||[]).map(n=>n.frequency||0));
  const mxE=Math.max(1,...(bEdges||[]).map(e=>e.frequency||0));

  const nodes=(bNodes||[]).map(n=>{
    const pos = dir === 'LR' ? (n.position_h || {x:0,y:0}) : (n.position_v || {x:0,y:0});
    return{
      id:n.id, type:'processNode',
      position: pos,
      data:{
        label:n.label,
        is_main:n.is_main,
        frequency:n.frequency||0, 
        maxFreq:mxF
      },
    };
  });

  const edges=(bEdges||[]).map(e=>{
    const sN=nodes.find(n=>n.id===e.source);
    const tN=nodes.find(n=>n.id===e.target);
    if(!sN||!tN) return null;

    const { sh, th, curvature, sweepSide, sweepDist } = classifyEdge(
      e.source, e.target, sN.position, tN.position, dir
    );

    return{
      id:e.id||`${e.source}--${e.target}`,
      source:e.source,target:e.target,
      sourceHandle:sh,targetHandle:th,
      type:'freqEdge',
      markerEnd:{type:MarkerType.ArrowClosed, color:'#605E5C'},
      data:{frequency:e.frequency,avg_days:e.avg_days,maxFreq:mxE, curvature, sweepSide, sweepDist},
    };
  }).filter(Boolean);

  setRfNodes(nodes);
  setRfEdges(edges);
};

/* ─── HELPERS ──────────────────────────────────────────────────── */
const VALID_KEYS=new Set(['customer','vkorg','auart','matkl','werks',
  'case_id','month','activity','year','quarter','lead_time', 'ernam', 'status']);
const qs=(params)=>{
  const p=Object.entries(params).filter(([k,v])=>VALID_KEYS.has(k)&&v&&v!=='ALL');
  return p.length?'?'+p.map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join('&'):'';
};

const CROSS_TO_PARAM={
  customer:'customer', vkorg:'vkorg', auart:'auart', matkl:'matkl', werks:'werks',
  activity:'activity', month:'month', year:'year', quarter:'quarter',
  case_id:'case_id', lead_time:'lead_time', ernam:'ernam', status:'status'
};

const Empty=()=>(
  <div style={{height:90,display:'flex',alignItems:'center',justifyContent:'center',
    color:C.slate,fontSize:12}}>No data available</div>
);

/* ─── KPI CARD ─────────────────────────────────────────────────── */
const KpiCard=({label,value,color,highlighted,onClick, tooltip})=>{
  const [hover,setHover]=useState(false);
  const bColor = hover ? 'rgba(0,107,60,0.5)' : (highlighted ? C.selectedBorder : 'transparent');
  const bWidth = '1.5px';

  return(
    <div onClick={onClick} onMouseEnter={()=>setHover(true)} onMouseLeave={()=>setHover(false)} style={{
      background:highlighted?C.selected:C.card,borderRadius:6,padding:'10px 14px',
      borderTop: `${bWidth} solid ${bColor}`, borderRight: `${bWidth} solid ${bColor}`,
      borderBottom: `${bWidth} solid ${bColor}`, borderLeft:`4px solid #006B3C`,
      boxShadow: hover ? '0 6px 16px rgba(0,107,60,.15)' : '0 2px 6px rgba(0,0,0,.05)',
      transition:'all .2s', cursor:onClick?'pointer':'default',minWidth:0, position:'relative',
      display:'flex',flexDirection:'column',alignItems:'center', textAlign:'center',
      transform: hover ? 'translateY(-3px)' : 'none', boxSizing: 'border-box',
      zIndex: hover ? 50 : 1 
    }}>
      <div style={{fontSize:10,fontWeight:600,color:"#006B3C",textTransform:'uppercase',
        letterSpacing:.5,marginBottom:2}}>{label}</div>
      <div style={{fontSize:20,fontWeight:600,color:'#000000',lineHeight:1}}> 
        {value!=null?value.toLocaleString():'—'}
      </div>
      {hover && tooltip && (
        <div style={{position:'absolute', top:'100%', left:0, marginTop:4,
          background:'#fff', border:`1px solid ${C.border}`, borderRadius:4,
          padding:'6px 10px', boxShadow:'0 4px 12px rgba(0,0,0,.15)',
          fontSize:11, color:'#323130', zIndex:100, whiteSpace:'nowrap', textAlign:'left'}}>
          {tooltip}
        </div>
      )}
    </div>
  );
};

const ConfKpiCard=({label,value,color,sub,tooltip,onClick,highlighted})=>{
  const [hover,setHover]=useState(false);
  const bColor = hover ? 'rgba(209,52,56,0.5)' : (highlighted ? C.selectedBorder : 'transparent');
  const bWidth = '1.5px';

  return(
    <div onClick={onClick} onMouseEnter={()=>setHover(true)} onMouseLeave={()=>setHover(false)} style={{
      background:highlighted?C.selected:C.card,borderRadius:6,padding:'10px 14px',
      borderTop: `${bWidth} solid ${bColor}`, borderRight: `${bWidth} solid ${bColor}`,
      borderBottom: `${bWidth} solid ${bColor}`, borderLeft:`4px solid #D13438`,
      boxShadow: hover ? '0 6px 16px rgba(209,52,56,.15)' : '0 2px 6px rgba(0,0,0,.05)',
      transition:'all .2s', cursor:onClick?'pointer':'default',minWidth:0, position:'relative',
      display:'flex',flexDirection:'column',alignItems:'center', textAlign:'center',
      transform: hover ? 'translateY(-3px)' : 'none', boxSizing: 'border-box',
      zIndex: hover ? 50 : 1 
    }}>
      <div style={{fontSize:10,fontWeight:600,color: "#D13438",textTransform:'uppercase',
        letterSpacing:.5,marginBottom:2}}>{label}</div>
      <div style={{fontSize:20,fontWeight:600,color:'#000000',lineHeight:1}}> 
        {value!=null?Number(value).toLocaleString():'—'}
      </div>
      {sub&&<div style={{fontSize:10,color:C.slate,marginTop:3}}>{sub}</div>}
      {hover && tooltip && (
        <div style={{position:'absolute', top:'100%', left:0, marginTop:4,
          background:'#fff', border:`1px solid ${C.border}`, borderRadius:4,
          padding:'6px 10px', boxShadow:'0 4px 12px rgba(0,0,0,.15)',
          fontSize:11, color:'#323130', zIndex:100, whiteSpace:'nowrap', textAlign:'left'}}>
          {tooltip}
        </div>
      )}
    </div>
  );
};

/* ─── SEARCHABLE SELECT COMPONENT ─────────────────────────────── */
const SearchableSelect=({label,value,options,onChange,wide})=>{
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

  const filteredOptions = ['ALL', ...options].filter(opt => 
    String(opt).toLowerCase().includes(search.toLowerCase())
  );

  return(
    <div style={{display:'flex',flexDirection:'column',gap:3, flex:1, minWidth: '150px', position:'relative'}} ref={dropdownRef}>
      <label style={{fontSize:10,fontWeight:700,color:'#323130',
        textTransform:'uppercase',letterSpacing:.4}}>{label}</label>
      
      <div 
        onClick={() => setIsOpen(!isOpen)}
        style={{
          fontSize:12, padding:'5px 8px', borderRadius:4, width: '100%',
          border: value&&value!=='ALL' ? `1.5px solid ${C.blue700}` : `1px solid ${C.border}`,
          background: value&&value!=='ALL' ? '#EFF6FF' : C.card,
          color: '#323130', cursor:'pointer', fontWeight: value&&value!=='ALL' ? 700 : 'normal',
          display:'flex', justifyContent:'space-between', alignItems:'center'
        }}
      >
        <span style={{whiteSpace:'nowrap', overflow:'hidden', textOverflow:'ellipsis'}}>{value || 'ALL'}</span>
        <span style={{fontSize: 10, opacity:0.6}}>▼</span>
      </div>

      {isOpen && (
        <div style={{
          position:'absolute', top:'100%', left:0, right:0, zIndex:1000,
          background:'#fff', border:`1px solid ${C.border}`, borderRadius:4,
          boxShadow:'0 4px 12px rgba(0,0,0,0.15)', maxHeight:200, overflowY:'auto',
          marginTop: 2
        }}>
          <input 
            type="text" 
            placeholder="Search..." 
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            style={{
              width:'100%', padding:'6px', border:'none', borderBottom:`1px solid ${C.border}`,
              fontSize:12, outline:'none'
            }}
            onClick={(e) => e.stopPropagation()} 
            autoFocus
          />
          {filteredOptions.map((opt, i) => (
            <div 
              key={i}
              onClick={() => { onChange(opt); setIsOpen(false); setSearch(''); }}
              style={{
                padding:'6px 8px', fontSize:12, cursor:'pointer',
                background: value === opt ? '#EFF6FF' : '#fff',
                color: value === opt ? C.blue700 : '#323130',
                borderLeft: value === opt ? `3px solid ${C.blue700}` : '3px solid transparent'
              }}
              onMouseEnter={(e) => { if(value!==opt) e.currentTarget.style.background = '#F3F2F1'; }}
              onMouseLeave={(e) => { if(value!==opt) e.currentTarget.style.background = '#fff'; }}
            >
              {opt}
            </div>
          ))}
          {filteredOptions.length === 0 && (
            <div style={{padding:'8px', fontSize:11, color:'#8A8886', textAlign:'center'}}>
              No results
            </div>
          )}
        </div>
      )}
    </div>
  );
};

const FilterSelect=({label,value,options,onChange,wide})=>(
  <div style={{display:'flex',flexDirection:'column',gap:3, flex:1, minWidth: '150px'}}>
    <label style={{fontSize:10,fontWeight:700,color:'#323130',
      textTransform:'uppercase',letterSpacing:.4}}>{label}</label>
    <select value={value} onChange={e=>onChange(e.target.value)} style={{
      fontSize:12,padding:'5px 8px',borderRadius:4, width: '100%',
      border:value&&value!=='ALL'?`1.5px solid ${C.blue700}`:`1px solid ${C.border}`,
      background:value&&value!=='ALL'?'#EFF6FF':C.card,
      color:'#323130',outline:'none',cursor:'pointer',
      fontWeight:value&&value!=='ALL'?700:'normal',
    }}>
      {(Array.isArray(options)?options:['ALL']).map(o=>(
        <option key={o} value={o}>{o}</option>
      ))}
    </select>
  </div>
);

const ChartCard = React.memo(({title,subtitle,children,highlighted,onClear,style={}, loading=false})=>(
  <div style={{background:C.card,borderRadius:8,padding:'12px 14px',
    border:highlighted?`1.5px solid ${C.selectedBorder}`:`1px solid ${C.border}`,
    boxShadow:'0 2px 8px rgba(0,0,0,.05)',transition:'all .2s',
    display:'flex',flexDirection:'column',...style}}>
    
    <div style={{display:'flex',justifyContent:'space-between',alignItems:'flex-start',marginBottom:8}}>
      <div>
        <div style={{fontSize:13,fontWeight:700,color:'#323130'}}>{title}</div>
        {subtitle&&<div style={{fontSize:10,color:'#8A8886',marginTop:2}}>{subtitle}</div>}
      </div>
      {highlighted&&onClear&&(
        <button onClick={onClear} style={{fontSize:11,color:'#fff',background:C.blue700,
          border:'none',borderRadius:4,padding:'3px 9px',cursor:'pointer',fontWeight:600,flexShrink:0}}>
          Clear
        </button>
      )}
    </div>

    <div style={{flex:1,minHeight:0, position:'relative'}}>
      {loading && (
        <div style={{
          position:'absolute',inset:0,zIndex:10,
          background:'rgba(255,255,255,0.85)',
          backdropFilter:'blur(2px)',
          display:'flex',flexDirection:'column',alignItems:'center',justifyContent:'center',gap:8
        }}>
          <div style={{
            width:20,height:20,borderRadius:'50%',
            border:`2px solid ${C.blue700}`, borderTopColor:'transparent',
            animation:'spin 0.8s linear infinite'
          }}/>
          <div style={{fontSize:11,fontWeight:600,color:C.blue700}}>Analysing...</div>
        </div>
      )}
      {children}
    </div>
  </div>
));

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

/* ══════════════════════════════════════════
   CHARTS (MEMOIZED FOR PERFORMANCE)
══════════════════════════════════════════ */

const GenericPieChart = React.memo(({data, nameKey, dataKey, crossFilter, onSelect}) => {
  if(!Array.isArray(data)||!data.length) return <Empty/>;
  const af=crossFilter?.type===nameKey?crossFilter.value:null;

  // Calculate total vs active count for the center text
  const total = data.reduce((sum, d) => sum + (d[dataKey] || 0), 0);
  const activeCount = af ? (data.find(d => d[nameKey] === af)?.[dataKey] || 0) : total;

  // Custom label renderer to show Percentages outside the slices
  const renderPieLabel = (props) => {
    const { cx, cy, midAngle, outerRadius, percent } = props;

    // HIDE label if the slice is less than 3% of the total pie to prevent overlap
    if (percent < 0.03) return null;

    const RADIAN = Math.PI / 180;
    const radius = outerRadius + 16; // Push text outside the slice
    const x = cx + radius * Math.cos(-midAngle * RADIAN);
    const y = cy + radius * Math.sin(-midAngle * RADIAN);

    return (
      <text
        x={x}
        y={y}
        fill={ACCENT[props.index % ACCENT.length]}
        textAnchor={x > cx ? 'start' : 'end'}
        dominantBaseline="central"
        style={{ fontSize: 11, fontWeight: 700 }}
      >
        {/* Render as Percentage instead of raw value */}
        {`${(percent * 100).toFixed(1)}%`}
      </text>
    );
  };

  const legendPayload = data.map((entry, i) => ({
    id: entry[nameKey],
    type: 'circle',
    value: entry[nameKey],
    color: (af && af !== entry[nameKey]) ? '#D2D0CE' : ACCENT[i % ACCENT.length],
  }));

  return(
    <div style={{width:'100%',height:220, position:'relative'}}>
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Pie data={data} dataKey={dataKey} nameKey={nameKey}
            cx="45%" cy="50%"
            innerRadius={50} outerRadius={75} /* Adjusted for a wider center hole */
            paddingAngle={2}
            labelLine={{ stroke: '#8A8886', strokeWidth: 1, strokeDasharray: '3 3' }} /* Dotted connector line */
            label={renderPieLabel}>
            {data.map((entry,i)=>(
              <Cell key={i} fill={ACCENT[i%ACCENT.length]} cursor="pointer"
                opacity={af&&af!==entry[nameKey]?0.3:1}
                stroke={af===entry[nameKey]?'#323130':'none'}
                strokeWidth={af===entry[nameKey]?2:0}
                onClick={()=>onSelect(nameKey, entry[nameKey]===af?null:entry[nameKey])}/>
            ))}
          </Pie>

          {/* --- Center Text for Total/Active Cases --- */}
          <text x="42.7%" y="46%" textAnchor="middle" dominantBaseline="middle" style={{fontSize:18,fontWeight:800,fill:'#323130'}}>
            {Number(activeCount).toLocaleString()}
          </text>
          <text x="42.7%" y="56%" textAnchor="middle" dominantBaseline="middle" style={{fontSize:10,fill:'#8A8886',fontWeight:600}}>
            {af ? 'Cases' : 'Total Cases'}
          </text>

          <Tooltip content={<CustomTooltip nameKey={nameKey} labelOverride="cases"/>} />
          <Legend
            payload={legendPayload}
            layout="vertical" verticalAlign="middle" align="right"
            wrapperStyle={{fontSize:'11px', cursor:'pointer'}}
            onClick={(entry) => { if (entry && entry.value) { onSelect(nameKey, entry.value === af ? null : entry.value); } }}
          />
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
});

const ActivityChart = React.memo(({data,crossFilter,onSelect})=>{
  if(!Array.isArray(data)||!data.length) return <Empty/>;
  const af=crossFilter?.type==='activity'?crossFilter.value:null;
  const rows=data.slice(0,8); 
  return(
    <div style={{display:'flex',flexDirection:'column',gap:6}}>
      <div style={{display:'flex',gap:14}}>
        {[['#006B3C','Events Occurred'],['#038387','Unique Cases']].map(([c,l])=>(
          <div key={l} style={{display:'flex',alignItems:'center',gap:4}}>
            <div style={{width:10,height:10,borderRadius:2,background:c}}/>
            <span style={{fontSize:10,color:C.slate}}>{l}</span>
          </div>
        ))}
      </div>
      <div style={{width:'100%',height:220}}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={rows} layout="vertical" margin={{left:40,right:20,top:4,bottom:4}}>
            <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" horizontal={false}/>
            <XAxis type="number" hide/>
            <YAxis type="category" dataKey="activity" tick={{fontSize:11,fill:'#605E5C'}} width={135} interval={0}/>
            <Tooltip cursor={{fill:'rgba(0,0,0,.03)'}} content={<CustomTooltip nameKey="activity"/>}/>
            
            <Bar dataKey="count" barSize={20}
              onClick={e=>e?.activity&&onSelect('activity',e.activity===af?null:e.activity)}>
              {rows.map((e,i)=>(
                <Cell key={i} cursor="pointer" fill={af===e?.activity?'#CA5010':'#006B3C'} opacity={af&&af!==e?.activity?0.35:1}/>
              ))}
            </Bar>
            
            <Bar dataKey="unique_cases" radius={[0,3,3,0]} barSize={20}
              onClick={e=>e?.activity&&onSelect('activity',e.activity===af?null:e.activity)}>
              {rows.map((e,i)=>(
                <Cell key={i} cursor="pointer" fill={af===e?.activity?'#999999':'#038387'} opacity={af&&af!==e?.activity?0.3:0.9}/>
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
});

const MonthlyChart = React.memo(({data,crossFilter,onSelect})=>{
  if(!Array.isArray(data)||!data.length) return <Empty/>;
  return(
    <div style={{width:'100%', height:220}}>
      <ResponsiveContainer width="100%" height="100%">
        <ComposedChart data={data} margin={{left:8,right:8,top:10,bottom:40}}>
          <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9"/>
          <XAxis dataKey="Month" tick={{fontSize:10,fill:'#605E5C'}} angle={-45} textAnchor="end" interval={0}/>
          <YAxis tick={{fontSize:10,fill:'#605E5C'}} width={40}/>
          <Tooltip content={<CustomTooltip nameKey="Month" labelOverride="cases"/>}/> 
          <Bar dataKey="count" fill="transparent" cursor="pointer"
            onClick={e=>e?.Month&&onSelect('month',e.Month===crossFilter?.value?null:e.Month)}/>
          
          <Line 
            type="monotone" dataKey="count" stroke="#006B3C" strokeWidth={2.5} cursor="pointer"
            dot={{r:3, fill:'#006B3C'}} 
            activeDot={{
                r:6, fill:'#CA5010', stroke:'#fff', strokeWidth:2, cursor: 'pointer',
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

const StatusDonutChart = React.memo(({data,crossFilter,onSelect})=>{
  if(!Array.isArray(data)||!data.length) return <Empty/>;
  const af=crossFilter?.type==='status'?crossFilter.value:null;
  const total=data.reduce((s,d)=>s+(d.count||0),0);

  const chartData = data.map((d) => ({
    ...d,
    fill: d.name === 'Happy Path' || d.status === 'Happy Path' ? '#107C10' : '#D13438'
  }));

  const renderLabel=({cx,cy,midAngle,innerRadius,outerRadius,percent})=>{
    if(percent<0.05) return null;
    const R=Math.PI/180,r=innerRadius+(outerRadius-innerRadius)*.55;
    const x=cx+r*Math.cos(-midAngle*R),y=cy+r*Math.sin(-midAngle*R);
    return(
      <text x={x} y={y} fill="#fff" textAnchor="middle" dominantBaseline="central"
        style={{fontSize:10,fontWeight:700,pointerEvents:'none'}}>
        {`${(percent*100).toFixed(0)}%`}
      </text>
    );
  };

  const legendPayload = chartData.map((entry) => ({
    id: entry.name || entry.status,
    type: 'circle',
    value: entry.name || entry.status,
    color: (af && af !== (entry.name || entry.status)) ? '#D2D0CE' : entry.fill,
  }));

  return(
    <div style={{width:'100%',height:260, position:'relative'}}>
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Pie data={chartData} dataKey="count" nameKey={data[0]?.name ? "name" : "status"}
            cx="40%" cy="50%" innerRadius={60} outerRadius={95}
            paddingAngle={2} labelLine={false} label={renderLabel}
            onClick={e=>e&&(e.name||e.status)&&onSelect('status',(e.name||e.status)===af?null:(e.name||e.status))}>
            {chartData.map((entry,i)=>(
              <Cell key={i} fill={entry.fill} cursor="pointer"
                opacity={af&&af!==(entry?.name||entry?.status)?0.2:1}
                stroke={af===(entry?.name||entry?.status)?'#323130':'none'}
                strokeWidth={af===(entry?.name||entry?.status)?2:0}/>
            ))}
          </Pie>
          <text x="36%" y="46%" textAnchor="middle" dominantBaseline="middle" style={{fontSize:18,fontWeight:800,fill:'#323130'}}>
            {Number(af?(chartData.find(d=>(d.name||d.status)===af)?.count||0):total).toLocaleString()}
          </text>
          <text x="36%" y="56%" textAnchor="middle" dominantBaseline="middle" style={{fontSize:10,fill:'#8A8886',fontWeight:600}}>
            {af||'Total Cases'}
          </text>
          <Tooltip formatter={v=>[Number(v).toLocaleString(),'Cases']} contentStyle={{background:'#fff',border:`1px solid ${C.border}`,borderRadius:6,fontSize:12}}/>
          <Legend
            payload={legendPayload}
            layout="vertical"
            verticalAlign="middle"
            align="right"
            wrapperStyle={{fontSize: '11px', cursor: 'pointer', right: 10}}
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

const ScrollableHBarChart = React.memo(({data,dataKey,labelKey,crossFilter,crossKey,onSelect,color})=>{
  if(!Array.isArray(data)||!data.length) return <Empty/>;
  const af=crossFilter?.type===crossKey?crossFilter.value:null;
  const sorted=[...data].sort((a,b)=>b[dataKey]-a[dataKey]);
  const rowH=30;
  const chartH=Math.max(220, sorted.length*rowH);
  return(
    <div style={{width:'100%',height:220, overflowY:'auto', paddingRight:8}}>
      <div style={{height:chartH}}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={sorted} layout="vertical" margin={{left:10,right:20,top:4,bottom:4}}>
            <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" horizontal={false}/>
            <XAxis type="number" hide/>
            <YAxis type="category" dataKey={labelKey} tick={{fontSize:10,fill:'#605E5C'}} width={120} interval={0}/>
            <Tooltip cursor={{fill:'rgba(0,0,0,.04)'}} content={<CustomTooltip nameKey={labelKey} labelOverride="cases"/>}/>
            <Bar dataKey={dataKey} radius={[0,3,3,0]} barSize={20}
              onClick={e=>e&&e[labelKey]&&onSelect(crossKey,e[labelKey]===af?null:e[labelKey])}>
              {sorted.map((entry,i)=>(
                <Cell key={i} cursor="pointer" fill={color||'#5C2D91'}
                  opacity={af&&af!==entry?.[labelKey]?0.25:1}/>
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
});

const ScrollableVBarChart = React.memo(({data,crossFilter,onSelect,dataKey='count',labelKey='auart'})=>{
  if(!Array.isArray(data)||!data.length) return <Empty/>;
  const af=crossFilter?.type===labelKey?crossFilter.value:null;
  const colW=50;
  const chartW=Math.max('100%', data.length*colW);
  return(
    <div style={{width:'100%',height:220, overflowX:'auto', overflowY:'hidden'}}>
      <div style={{width:chartW, height:'100%'}}>
        <ResponsiveContainer width="100%" height="110%">
          <BarChart data={data} margin={{left:8,right:16,top:10,bottom:40}}>
            <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" vertical={false}/>
            <XAxis dataKey={labelKey} tick={{fontSize:11,fill:'#605E5C'}} angle={-40} textAnchor="end" interval={0}/>
            <YAxis tick={{fontSize:10,fill:'#605E5C'}} width={40}/>
            <Tooltip cursor={{fill:'rgba(0,0,0,.04)'}} content={<CustomTooltip nameKey={labelKey} labelOverride="cases"/>}/>
            <Bar dataKey={dataKey} radius={[4,4,0,0]}
              onClick={e=>e&&e[labelKey]&&onSelect(labelKey,e[labelKey]===af?null:e[labelKey])}>
              {data.map((entry,i)=>(
                <Cell key={i} cursor="pointer" fill={ACCENT[i%ACCENT.length]} opacity={af&&af!==entry?.[labelKey]?0.25:1}/>
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
});

const LeadTimeChart = React.memo(({data, crossFilter, onSelect})=>{
  if(!Array.isArray(data)||!data.length) return <Empty/>;
  const af = crossFilter?.type === 'lead_time' ? crossFilter.value : null;

  return(
    <div style={{width:'100%', height:235}}>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{left:8,right:8,top:8,bottom:40}}>
          <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" vertical={false}/>
          <XAxis dataKey="label" tick={{fontSize:9,fill:'#605E5C'}} angle={-45} textAnchor="end" interval={1}/>
          <YAxis tick={{fontSize:10,fill:'#605E5C'}} width={40}/>
          <Tooltip content={<CustomTooltip nameKey="label" labelOverride="cases"/>}/>
          <Bar dataKey="count" radius={[2,2,0,0]}
               onClick={e => e?.label && onSelect('lead_time', e.label === af ? null : e.label)}>
             {data.map((entry, i) => (
                <Cell key={i} cursor="pointer" fill={af === entry.label ? '#CA5010' : '#038387'}
                  opacity={af && af !== entry.label ? 0.35 : 1}/>
             ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
});

const ErnamChart = React.memo(({data, crossFilter, onSelect})=>{
    if(!Array.isArray(data)||!data.length) return <Empty/>;
    
    const af = crossFilter?.type === 'ernam' ? crossFilter.value : null;
    const sorted = [...data].sort((a,b)=>b.count-a.count);
    const rowH=30;
    const chartH=Math.max(220, sorted.length*rowH);

    return(
      <div style={{width:'100%',height:220, overflowY:'auto', paddingRight:8}}>
        <div style={{height:chartH}}>
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={sorted} layout="vertical" margin={{left:10,right:20,top:4,bottom:4}}>
              <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" horizontal={false}/>
              <XAxis type="number" hide/>
              <YAxis type="category" dataKey="ernam" tick={{fontSize:10,fill:'#605E5C'}} width={90} interval={0}/>
              <Tooltip cursor={{fill:'rgba(0,0,0,.04)'}} content={<CustomTooltip nameKey="ernam" labelOverride="cases"/>}/>
              <Bar dataKey="count" radius={[0,3,3,0]} barSize={20}
                  onClick={e => e?.ernam && onSelect('ernam', e.ernam === af ? null : e.ernam)}>
                  {sorted.map((entry,i)=>(
                    <Cell key={i} cursor="pointer" fill="#006B3C" opacity={af&&af!==entry?.ernam?0.25:1}/>
                  ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    );
});

/* ─── SoD VERTICAL BAR CHART ───────────────────────────────────── */
const SodChart = React.memo(({ data }) => {
  if (!Array.isArray(data) || !data.length) return <Empty />;
  return (
    <div style={{ width: '100%', height: 260 }}>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ left: 8, right: 16, top: 16, bottom: 70 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" vertical={false} />
          <XAxis dataKey="violation" tick={{ fontSize: 11, fill: '#605E5C' }} angle={-25} textAnchor="end" interval={0} />
          <YAxis tick={{ fontSize: 10, fill: '#605E5C' }} width={44} />
          <Tooltip cursor={{ fill: 'rgba(0,0,0,.04)' }} content={<CustomTooltip nameKey="violation" labelOverride="cases" />} />
          <Bar dataKey="count" radius={[4, 4, 0, 0]} barSize={40} fill="#D13438">
            {data.map((entry, i) => (
              <Cell key={i} fill="#D13438" />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
});

const DeviationsSummaryChart = React.memo(({ data }) => {
  if (!Array.isArray(data) || !data.length) return <Empty />;
  const DEV_COLORS = {
    'SO Reversed':      '#D13438',
    'SO Rev After GI':  '#E81123',
    'Delivery Returned':'#E3008C',
    'GI Reversed':      '#CA5010',
    'Invoice Reversed': '#5C2D91',
    'Credit Memo':      '#038387',
    'Debit Memo':       '#F59E0B',
  };
  return (
    <div style={{ width: '100%', height: 220 }}>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} layout="vertical" margin={{ left: 10, right: 30, top: 4, bottom: 4 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" horizontal={false} />
          <XAxis type="number" hide />
          <YAxis type="category" dataKey="deviation" tick={{ fontSize: 10, fill: '#605E5C' }} width={130} interval={0} />
          <Tooltip cursor={{ fill: 'rgba(0,0,0,.04)' }} content={<CustomTooltip nameKey="deviation" labelOverride="cases" />} />
          <Bar dataKey="count" radius={[0, 4, 4, 0]} barSize={22}>
            {data.map((entry, i) => (
              <Cell key={i} fill={DEV_COLORS[entry.deviation] || ACCENT[i % ACCENT.length]} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
});

/* ─── BOTTLENECK CHART ─────────────────────────────────────────── */
const BottleneckTooltip = ({ active, payload }) => {
  if (!active || !payload?.length) return null;
  const d = payload[0].payload;
  return (
    <div style={{ background: 'rgba(255,255,255,.98)', border: `1px solid #E1DFDD`, borderRadius: 6, padding: '8px 14px', fontSize: 12, color: '#323130' }}>
      <div style={{ fontWeight: 700, color: '#006B3C', marginBottom: 4 }}>{d.step}</div>
      <div style={{ color: '#605E5C' }}>Avg Days: <strong style={{ color: '#323130' }}>{d.avg_days}</strong></div>
      <div style={{ color: '#605E5C' }}>Median Days: <strong style={{ color: '#038387' }}>{d.median_days}</strong></div>
      <div style={{ color: '#605E5C' }}>Cases: <strong>{Number(d.count).toLocaleString()}</strong></div>
    </div>
  );
};

const BottleneckChart = React.memo(({ data }) => {
  if (!Array.isArray(data) || !data.length) return <Empty />;
  return (
    <div style={{ width: '100%', height: 260 }}>
      <ResponsiveContainer width="100%" height="100%">
        <ComposedChart data={data} margin={{ left: 8, right: 20, top: 8, bottom: 85 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" vertical={false} />
          <XAxis dataKey="step" tick={{ fontSize: 10, fill: '#605E5C' }} angle={-35} textAnchor="end" interval={0} />
          <YAxis tick={{ fontSize: 10, fill: '#605E5C' }} width={45} label={{ value: 'Days', angle: -90, position: 'insideLeft', offset: 5, style: { fontSize: 10, fill: '#605E5C' } }} />
          <Tooltip content={<BottleneckTooltip />} />
          <Bar dataKey="avg_days" name="Avg Days" radius={[4, 4, 0, 0]} fill="#006B3C" />
          <Line type="monotone" dataKey="median_days" name="Median Days" stroke="#CA5010" strokeWidth={2} dot={{ fill: '#CA5010', r: 4 }} />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
});

/* ─── CUSTOMER AVG SO-TO-PAYMENT DAYS CHART ─────────────────────── */
const CustomerAvgDaysTooltip = ({ active, payload }) => {
  if (!active || !payload?.length) return null;
  const d = payload[0].payload;
  return (
    <div style={{ background: 'rgba(255,255,255,.98)', border: `1px solid #E1DFDD`, borderRadius: 6, padding: '8px 14px', fontSize: 12, color: '#323130', maxWidth: 240 }}>
      <div style={{ fontWeight: 700, color: '#038387', marginBottom: 4, wordBreak: 'break-word' }}>{d.customer}</div>
      <div style={{ color: '#605E5C' }}>Avg Cycle Days: <strong style={{ color: '#323130' }}>{d.avg_days}d</strong></div>
      <div style={{ color: '#605E5C', marginTop: 2 }}>Cases: <strong>{Number(d.case_count).toLocaleString()}</strong></div>
    </div>
  );
};

const CustomerAvgDaysChart = React.memo(({ data, crossFilter, onSelect }) => {
  if (!Array.isArray(data) || !data.length) return <Empty />;

  const af = crossFilter?.type === 'customer' ? crossFilter.value : null;
  const sorted = [...data].sort((a, b) => b.avg_days - a.avg_days);

  const colW = 72;
  const chartW = Math.max(500, sorted.length * colW);

  return (
    <div style={{ width: '100%', height: 260, overflowX: 'auto', overflowY: 'hidden', minWidth: 0, maxWidth: '100%' }}>
      <div style={{ width: chartW, height: '100%', minWidth: chartW }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={sorted} margin={{ left: 8, right: 16, top: 16, bottom: 52 }}>
            <defs>
              <linearGradient id="custAvgGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#038387" stopOpacity={1} />
                <stop offset="100%" stopColor="#00B7C3" stopOpacity={0.7} />
              </linearGradient>
              <linearGradient id="custAvgGradActive" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#CA5010" stopOpacity={1} />
                <stop offset="100%" stopColor="#F59E0B" stopOpacity={0.8} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" vertical={false} />
            <XAxis
              dataKey="customer"
              tick={{ fontSize: 10, fill: '#605E5C' }}
              angle={-38} textAnchor="end" interval={0}
              tickFormatter={v => v && v.length > 14 ? v.slice(0, 13) + '…' : v}
            />
            <YAxis
              tick={{ fontSize: 10, fill: '#605E5C' }}
              width={44}
              label={{ value: 'Avg Days', angle: -90, position: 'insideLeft', offset: 8, style: { fontSize: 10, fill: '#8A8886' } }}
            />
            <Tooltip content={<CustomerAvgDaysTooltip />} cursor={{ fill: 'rgba(3,131,135,.06)' }} />
            <Bar
              dataKey="avg_days"
              radius={[5, 5, 0, 0]}
              onClick={e => e?.customer && onSelect('customer', e.customer === af ? null : e.customer)}
            >
              {sorted.map((entry, i) => (
                <Cell
                  key={i}
                  cursor="pointer"
                  fill={
                    af === entry.customer
                      ? 'url(#custAvgGradActive)'
                      : 'url(#custAvgGrad)'
                  }
                  opacity={af && af !== entry.customer ? 0.28 : 1}
                  stroke={af === entry.customer ? '#CA5010' : 'none'}
                  strokeWidth={af === entry.customer ? 1.5 : 0}
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
/* ─── O2C INTRO SCREEN ──────────────────────────────────────────────────── */
/* ── O2C FAQ Item (reuses same pattern as P2P) ── */
const O2CFaqItem = ({ q, a, bullets }) => {
  const [open, setOpen] = useState(false);
  return (
    <div style={{borderBottom:'1px solid #E2E8F0'}}>
      <button
        onClick={() => setOpen(o => !o)}
        style={{
          width:'100%', display:'flex', justifyContent:'space-between', alignItems:'center',
          padding:'14px 0', background:'none', border:'none', cursor:'pointer', textAlign:'left', gap:12,
        }}>
        <span style={{fontSize:13.5, fontWeight:600, color:'#1e293b', lineHeight:1.4}}>{q}</span>
        <span style={{
          fontSize:18, color:'#006B3C', flexShrink:0, fontWeight:700,
          transform: open ? 'rotate(45deg)' : 'none', transition:'transform 0.2s',
          display:'inline-block', width:20, textAlign:'center',
        }}>+</span>
      </button>
      {open && (
        <div style={{paddingBottom:16, fontSize:13, color:'#475569', lineHeight:1.75}}>
          {a && <p style={{margin:'0 0 8px'}}>{a}</p>}
          {bullets && bullets.length > 0 && (
            <ul style={{margin:0, paddingLeft:20, display:'flex', flexDirection:'column', gap:5}}>
              {bullets.map((b, i) => (
                <li key={i} style={{color:'#334155', lineHeight:1.6}}>
                  {typeof b === 'object' && b.bold
                    ? <><strong style={{color:'#1e293b'}}>{b.bold}</strong>{b.rest}</>
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

const O2C_FAQS = [
  {
    q: 'What is the Order-to-Cash (O2C) process?',
    a: 'The O2C process covers the end-to-end workflow from receiving a customer order to collecting payment. It bridges sales, operations, logistics, billing, and finance. Key stages include:',
    bullets: [
      { bold: 'Order Creation:', rest: ' Sales order entry, credit check, and order confirmation' },
      { bold: 'Order Processing:', rest: ' Fulfillment — picking, packing, and shipping/delivery' },
      { bold: 'Invoicing/Billing:', rest: ' Invoice creation and dispatch to the customer' },
      { bold: 'Cash Collection:', rest: ' Payment receipt, accounting, and dunning for overdue invoices' },
      { bold: 'Reconciliation:', rest: ' Matching payments to invoices and closing the case' },
    ],
  },
  {
    q: 'What is Process Mining, and how does it apply to O2C?',
    a: 'Process Mining uses event log data from ERP (SAP, Oracle), CRM, and billing systems to reconstruct and visualize actual O2C execution. In O2C it builds a "digital twin" of the customer journey, quantifying:',
    bullets: [
      'Deviations from the ideal order-to-cash flow',
      'Bottlenecks causing delivery delays or invoice backlogs',
      'Invoice errors and rework loops that increase DSO',
      'Slow collections that impact working capital and cash flow',
    ],
  },
  {
    q: 'Why use Process Mining specifically for O2C?',
    a: 'O2C spans multiple departments and systems, creating silos and invisible revenue leaks. Process Mining delivers:',
    bullets: [
      { bold: 'Real visibility:', rest: ' Actual performance, not just designed or reported metrics' },
      { bold: 'Compliance detection:', rest: ' Orders bypassing credit checks or approval workflows' },
      { bold: 'Cash-flow leak identification:', rest: ' Missed payment terms, overdue invoices, billing errors' },
      { bold: 'Benchmarking:', rest: ' Compare across customers, regions, sales teams, or products' },
      { bold: 'Automation targeting:', rest: ' Prioritize RPA or AI-driven actions based on root cause data' },
    ],
  },
  {
    q: 'What are the most common pain points Process Mining uncovers in O2C?',
    a: 'Typical findings include:',
    bullets: [
      'Order blocks and rejections — credit holds, incomplete data, pricing errors',
      'Delivery delays, stockouts, and fulfillment bottlenecks',
      'Invoice inaccuracies — wrong terms, pricing mismatches, duplicate invoices',
      'Long invoicing or billing cycle times extending DSO',
      'Manual rework loops caused by data entry or system integration errors',
    ],
  },
  {
    q: 'What key metrics (KPIs) does Process Mining help track in O2C?',
    a: 'Common KPIs tracked include:',
    bullets: [
      { bold: 'Days Sales Outstanding (DSO):', rest: ' Average days from invoice to payment receipt' },
      { bold: 'End-to-End O2C Cycle Time:', rest: ' Order receipt through to cash collection' },
      { bold: 'On-Time Delivery (OTD) Rate:', rest: ' % of orders delivered by the promised date' },
      { bold: 'Order Fulfillment Cycle Time:', rest: ' From order creation to physical delivery' },
      { bold: 'Invoice Accuracy Rate:', rest: ' % of invoices issued without errors or disputes' },
    ],
  },
  {
    q: 'What are the top use cases for Process Mining in O2C?',
    a: 'Key use cases include:',
    bullets: [
      'Reducing DSO and accelerating cash collection by prioritising high-risk overdue invoices',
      'Improving on-time delivery and fulfillment performance',
      'Increasing invoice accuracy and reducing disputes and chargebacks',
      'Minimising order blocks, rejections, and credit management delays',
      'Enhancing customer experience through faster, error-free processes',
      'Continuous compliance monitoring across the revenue cycle',
    ],
  },
  {
    q: 'What data is required for Process Mining in O2C?',
    a: 'Core event logs need:',
    bullets: [
      { bold: 'Case ID:', rest: ' A unique identifier such as the sales order number' },
      { bold: 'Activity + Timestamp:', rest: ' e.g., "Create Sales Order", "Goods Issue", "Invoice Posted", "Payment Received"' },
      { bold: 'Optional attributes:', rest: ' Customer, sales rep, product, value, delivery date, invoice terms, payment method' },
      { bold: 'Data sources:', rest: ' SAP (VBAK, VBAP, VBFA, LIKP, VBRK), CRM (Salesforce), billing systems, WMS, and AR tools' },
    ],
  },
  {
    q: 'What challenges come with applying Process Mining to O2C?',
    a: 'Common challenges include:',
    bullets: [
      'Data extraction complexity — O2C event logs span multiple systems (ERP, CRM, WMS, AR)',
      'Data quality issues — incomplete timestamps or missing handoff events between systems',
      'Change management — sales and finance teams resisting visibility into actual process performance',
      'Scope definition — starting too broadly instead of targeting DSO reduction or dispute resolution',
      'Integration with action tools for automated dunning, credit management, or order routing',
    ],
  },
];

const O2CIntroScreen = ({ onGoTableBuild, onGoCsvUpload }) => {
  const [introStep, setIntroStep] = useState('overview'); // 'overview' | 'choose'

  const steps = [
    'Customer places an order — Sales Order (SO) is created in SAP',
    'SO is approved; any delivery or billing blocks are resolved',
    'Delivery document is created and goods are picked and packed',
    'Goods Issue (GI) is posted — inventory is reduced',
    'Billing document (invoice) is created and sent to customer',
    'Customer payment is received and the invoice is cleared in FI',
  ];
  const kpis = [
    'Order-to-delivery lead time','Invoice-to-cash cycle time',
    'Delivery block and billing block rate','Goods Issue reversal frequency',
    'Invoice reversal and credit memo rate','Days Sales Outstanding (DSO)',
  ];

  if (introStep === 'overview') return (
    <div style={{flex:1,display:'flex',flexDirection:'column',alignItems:'center',padding:'36px 24px 48px',overflowY:'auto'}}>
      <div style={{maxWidth:880,width:'100%',display:'flex',flexDirection:'column',gap:24}}>

        {/* Header */}
        <div style={{borderBottom:'2px solid #E2E8F0',paddingBottom:20}}>
          <div style={{fontSize:11,fontWeight:700,color:'#006B3C',textTransform:'uppercase',letterSpacing:1,marginBottom:6}}>Process Overview</div>
          <h1 style={{margin:'0 0 8px',fontSize:24,fontWeight:700,color:'#1e293b'}}>Order-to-Cash (O2C)</h1>
          <p style={{margin:0,fontSize:13.5,color:'#475569',lineHeight:1.7,maxWidth:720}}>
            The Order-to-Cash process covers the end-to-end revenue cycle — from a customer placing an order
            through delivery of goods, invoicing, and finally receiving payment. Process mining on O2C data
            reveals bottlenecks such as delivery delays, invoice reversals, sequence violations (invoicing
            before goods issue), and segregation-of-duties breaches.
          </p>
        </div>

        {/* Process Steps + KPIs */}
        <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:20}}>
          <div style={{background:'#fff',border:'1px solid #E2E8F0',borderRadius:10,padding:'20px 22px',boxShadow:'0 2px 6px rgba(0,0,0,0.04)'}}>
            <div style={{fontSize:11,fontWeight:700,color:'#64748b',textTransform:'uppercase',letterSpacing:0.8,marginBottom:14}}>Process Steps</div>
            <ol style={{margin:0,padding:'0 0 0 18px',display:'flex',flexDirection:'column',gap:10}}>
              {steps.map((s,i)=>(<li key={i} style={{fontSize:13,color:'#334155',lineHeight:1.5}}><span style={{fontWeight:600,color:'#006B3C'}}>Step {i+1}.</span>{' '}{s}</li>))}
            </ol>
          </div>
          <div style={{background:'#fff',border:'1px solid #E2E8F0',borderRadius:10,padding:'20px 22px',boxShadow:'0 2px 6px rgba(0,0,0,0.04)'}}>
            <div style={{fontSize:11,fontWeight:700,color:'#64748b',textTransform:'uppercase',letterSpacing:0.8,marginBottom:14}}>Key Metrics Analysed</div>
            <ul style={{margin:0,padding:'0 0 0 18px',display:'flex',flexDirection:'column',gap:10}}>
              {kpis.map((k,i)=>(<li key={i} style={{fontSize:13,color:'#334155',lineHeight:1.5}}>{k}</li>))}
            </ul>
          </div>
        </div>

        {/* FAQ Section */}
        <div style={{background:'#fff',border:'1px solid #E2E8F0',borderRadius:10,padding:'20px 24px',boxShadow:'0 2px 6px rgba(0,0,0,0.04)'}}>
          <div style={{fontSize:11,fontWeight:700,color:'#64748b',textTransform:'uppercase',letterSpacing:0.8,marginBottom:4}}>Frequently Asked Questions</div>
          <p style={{fontSize:12,color:'#94a3b8',margin:'0 0 16px'}}>Click a question to expand the answer</p>
          <div>
            {O2C_FAQS.map((faq, i) => <O2CFaqItem key={i} q={faq.q} a={faq.a} bullets={faq.bullets} />)}
          </div>
        </div>

        {/* Continue button — bottom right */}
        <div style={{display:'flex',justifyContent:'flex-end',paddingTop:4}}>
          <button
            onClick={() => setIntroStep('choose')}
            style={{
              background:'#006B3C', color:'#fff', border:'none',
              padding:'12px 32px', borderRadius:8, fontSize:14, fontWeight:700,
              cursor:'pointer', display:'flex', alignItems:'center', gap:8,
              boxShadow:'0 4px 12px rgba(0,107,60,0.25)', transition:'all 0.2s',
            }}
            onMouseOver={e=>{e.currentTarget.style.background='#004d2c';e.currentTarget.style.boxShadow='0 6px 16px rgba(0,107,60,0.35)';}}
            onMouseOut={e=>{e.currentTarget.style.background='#006B3C';e.currentTarget.style.boxShadow='0 4px 12px rgba(0,107,60,0.25)';}}>
            Continue
            <span style={{fontSize:16}}>→</span>
          </button>
        </div>
      </div>
    </div>
  );

  /* ── Choose your path ── */
  return (
    <div style={{flex:1,display:'flex',flexDirection:'column',alignItems:'center',padding:'36px 24px 48px',overflowY:'auto'}}>
      <div style={{maxWidth:880,width:'100%',display:'flex',flexDirection:'column',gap:24}}>

        {/* Back link */}
        <div>
          <button
            onClick={() => setIntroStep('overview')}
            style={{background:'none',border:'1px solid #E2E8F0',padding:'6px 14px',borderRadius:6,
              fontSize:12,cursor:'pointer',color:'#64748b',fontWeight:600}}
            onMouseOver={e=>e.currentTarget.style.background='#F8FAFC'}
            onMouseOut={e=>e.currentTarget.style.background='none'}>
            ← Back to Overview
          </button>
        </div>

        <div style={{textAlign:'center'}}>
          <div style={{fontSize:11,fontWeight:700,color:'#64748b',textTransform:'uppercase',letterSpacing:0.8,marginBottom:8}}>Get Started</div>
          <h2 style={{margin:'0 0 6px',fontSize:20,fontWeight:700,color:'#1e293b'}}>Choose how to load your data</h2>
          <p style={{margin:0,fontSize:13,color:'#64748b'}}>Select the method that matches your data format</p>
        </div>

        <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:20}}>
          {/* Build Event Log */}
          <div style={{background:'#fff',border:'2px solid #E2E8F0',borderRadius:12,padding:'28px 24px',
            display:'flex',flexDirection:'column',alignItems:'center',gap:14,textAlign:'center',
            boxShadow:'0 2px 8px rgba(0,0,0,0.05)',transition:'all 0.2s',cursor:'pointer'}}
            onClick={onGoTableBuild}
            onMouseEnter={e=>{e.currentTarget.style.borderColor='#006B3C';e.currentTarget.style.boxShadow='0 4px 16px rgba(0,107,60,0.15)';}}
            onMouseLeave={e=>{e.currentTarget.style.borderColor='#E2E8F0';e.currentTarget.style.boxShadow='0 2px 8px rgba(0,0,0,0.05)';}}>
            <div style={{width:60,height:60,borderRadius:14,background:'#EDFAF4',display:'flex',alignItems:'center',justifyContent:'center',fontSize:28}}>🔨</div>
            <div>
              <div style={{fontSize:16,fontWeight:700,color:'#1e293b',marginBottom:8}}>Build Event Log</div>
              <div style={{fontSize:13,color:'#64748b',lineHeight:1.65}}>
                Upload raw SAP tables (VBAK, VBAP, VBFA, LIKP, LIPS, VBRK, VBRP, BSAD, KNA1) and
                automatically build the O2C process event log.
              </div>
            </div>
            <button onClick={e=>{e.stopPropagation();onGoTableBuild();}}
              style={{background:'#006B3C',color:'#fff',border:'none',padding:'11px 28px',
                borderRadius:7,fontSize:13,fontWeight:700,cursor:'pointer',width:'100%',marginTop:'auto'}}
              onMouseOver={e=>e.currentTarget.style.background='#004d2c'}
              onMouseOut={e=>e.currentTarget.style.background='#006B3C'}>
              ⚡ Build Event Log
            </button>
          </div>

          {/* Upload Pre-built CSV */}
          <div style={{background:'#fff',border:'2px solid #E2E8F0',borderRadius:12,padding:'28px 24px',
            display:'flex',flexDirection:'column',alignItems:'center',gap:14,textAlign:'center',
            boxShadow:'0 2px 8px rgba(0,0,0,0.05)',transition:'all 0.2s',cursor:'pointer'}}
            onClick={onGoCsvUpload}
            onMouseEnter={e=>{e.currentTarget.style.borderColor='#0078D4';e.currentTarget.style.boxShadow='0 4px 16px rgba(0,120,212,0.15)';}}
            onMouseLeave={e=>{e.currentTarget.style.borderColor='#E2E8F0';e.currentTarget.style.boxShadow='0 2px 8px rgba(0,0,0,0.05)';}}>
            <div style={{width:60,height:60,borderRadius:14,background:'#EFF6FF',display:'flex',alignItems:'center',justifyContent:'center',fontSize:28}}>📂</div>
            <div>
              <div style={{fontSize:16,fontWeight:700,color:'#1e293b',marginBottom:8}}>Upload Pre-built CSV</div>
              <div style={{fontSize:13,color:'#64748b',lineHeight:1.65}}>
                Already have a formatted event log? Upload your pre-built wide-format CSV directly to launch the dashboard.
              </div>
            </div>
            <button onClick={e=>{e.stopPropagation();onGoCsvUpload();}}
              style={{background:'#0078D4',color:'#fff',border:'none',padding:'11px 28px',
                borderRadius:7,fontSize:13,fontWeight:700,cursor:'pointer',width:'100%',marginTop:'auto'}}
              onMouseOver={e=>e.currentTarget.style.background='#005A9E'}
              onMouseOut={e=>e.currentTarget.style.background='#0078D4'}>
              📂 Upload Pre-built CSV
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

/* ── O2C Table Upload Screen ─────────────────────────────────────────────── */
const O2CTableUploadScreen = ({ onBuilt, onBack, onLoadingChange, currentUser, myFiles, fetchingFiles, handleLoadOldFile }) => {
  const tables = [
    { name:'VBAK', desc:'Sales Document Header' },
    { name:'VBAP', desc:'Sales Document Item' },
    { name:'VBFA', desc:'Sales Document Flow (process links)' },
    { name:'LIKP', desc:'Delivery Header' },
    { name:'LIPS', desc:'Delivery Item' },
    { name:'VBRK', desc:'Billing Document Header' },
    { name:'VBRP', desc:'Billing Document Item' },
    { name:'BSAD', desc:'Cleared Customer Items (FI)' },
    { name:'KNA1', desc:'Customer Master — General' },
  ];
  const [tableStatus, setTableStatus] = useState(Object.fromEntries(tables.map(t=>[t.name,'idle'])));
  const [tableMsg,    setTableMsg]    = useState(Object.fromEntries(tables.map(t=>[t.name,''])));
  const [building,    setBuilding]    = useState(false);
  const [buildMsg,    setBuildMsg]    = useState('');
  const fileRefs = useRef(Object.fromEntries(tables.map(t=>[t.name,React.createRef()])));

  const allDone      = tables.every(t=>tableStatus[t.name]==='done');
  const anyUploading = tables.some(t=>tableStatus[t.name]==='uploading')||building;
  const tableBuilds  = (myFiles||[]).filter(f=>f.source==='table_build');

  const uploadTable = async(tName, file) => {
    if(!file) return;
    if(!file.name.toLowerCase().endsWith('.csv')){
      setTableStatus(p=>({...p,[tName]:'error'})); setTableMsg(p=>({...p,[tName]:'Only .csv accepted.'})); return;
    }
    setTableStatus(p=>({...p,[tName]:'uploading'})); setTableMsg(p=>({...p,[tName]:''}));
    const form=new FormData();
    form.append('file',file); form.append('table_name',tName); form.append('username',currentUser||'Unknown');
    try{
      const r=await fetch(`${API}/o2c/transform/upload_table`,{method:'POST',body:form});
      const d=await r.json();
      if(!r.ok) throw new Error(d.detail||`HTTP ${r.status}`);
      setTableStatus(p=>({...p,[tName]:'done'}));
      setTableMsg(p=>({...p,[tName]:`${Number(d.rows).toLocaleString()} rows`}));
    }catch(e){
      setTableStatus(p=>({...p,[tName]:'error'})); setTableMsg(p=>({...p,[tName]:e.message}));
    }
  };

  const handleBuild=async()=>{
    setBuilding(true); setBuildMsg('');
    onLoadingChange&&onLoadingChange(true,20,'Processing Data...');
    let prog=20;
    const ticker=setInterval(()=>{prog=Math.min(prog+Math.random()*14,88);onLoadingChange&&onLoadingChange(true,prog,'Analysing Data...');},400);
    try{
      const r=await fetch(`${API}/o2c/transform/build?username=${encodeURIComponent(currentUser||'Unknown')}`,{method:'POST'});
      const d=await r.json(); clearInterval(ticker);
      if(!r.ok) throw new Error(d.detail||`HTTP ${r.status}`);
      onLoadingChange&&onLoadingChange(true,100,'Dashboard Created');
      setTimeout(()=>{ setBuilding(false); setBuildMsg(`✓ ${Number(d.rows).toLocaleString()} rows processed`); if(onBuilt) onBuilt(); setTimeout(()=>{ onLoadingChange&&onLoadingChange(false,0,''); }, 500); },800);
    }catch(e){
      clearInterval(ticker); onLoadingChange&&onLoadingChange(false,0,'');
      setBuilding(false); setBuildMsg(`Error: ${e.message}`);
    }
  };

  const si=s=>{
    if(s==='done')     return{icon:'✓',color:'#107C10',bg:'#F0FAF0',border:'#107C10'};
    if(s==='error')    return{icon:'✕',color:'#D13438',bg:'#FDE7E9',border:'#D13438'};
    if(s==='uploading')return{icon:'…',color:'#006B3C',bg:'#EDFAF4',border:'#006B3C'};
    return                   {icon:'↑',color:'#006B3C',bg:'#fff',   border:'#006B3C'};
  };

  return(
    <div style={{flex:1,display:'flex',flexDirection:'column',alignItems:'center',padding:'28px 24px 48px',overflowY:'auto'}}>
      <div style={{maxWidth:820,width:'100%',display:'flex',flexDirection:'column',gap:20}}>
        <div style={{display:'flex',alignItems:'center',gap:12}}>
          <button onClick={onBack}
            style={{background:'none',border:'1px solid #E2E8F0',padding:'6px 14px',borderRadius:6,fontSize:12,cursor:'pointer',color:'#64748b',fontWeight:600}}
            onMouseOver={e=>e.currentTarget.style.background='#F8FAFC'}
            onMouseOut={e=>e.currentTarget.style.background='none'}>
            ← Back
          </button>
          <div>
            <div style={{fontSize:11,fontWeight:700,color:'#006B3C',textTransform:'uppercase',letterSpacing:0.8}}>Build Event Log</div>
            <div style={{fontSize:13,color:'#64748b'}}>Upload the 9 SAP tables below, then click Build</div>
          </div>
        </div>

        {/* Table upload panel */}
        <div style={{background:'#fff',border:'1px solid #E2E8F0',borderRadius:10,padding:'20px 22px',boxShadow:'0 2px 6px rgba(0,0,0,0.04)'}}>
          <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:14}}>
            <div style={{fontSize:11,fontWeight:700,color:'#64748b',textTransform:'uppercase',letterSpacing:0.8}}>Required SAP Tables</div>
            <div style={{fontSize:11,color:'#94a3b8'}}>Upload each as <strong style={{color:'#475569'}}>.csv</strong></div>
          </div>
          <div style={{display:'flex',flexDirection:'column',border:'1px solid #E2E8F0',borderRadius:8,overflow:'hidden'}}>
            {tables.map((t,i)=>{
              const s=si(tableStatus[t.name]);
              const ref=fileRefs.current[t.name];
              const isUp=tableStatus[t.name]==='uploading';
              return(
                <div key={t.name} style={{display:'flex',alignItems:'center',gap:12,padding:'10px 14px',
                  background:tableStatus[t.name]==='done'?'#F0FAF0':tableStatus[t.name]==='error'?'#FDE7E9':i%2===0?'#F8FAFC':'#fff',
                  borderBottom:i<tables.length-1?'1px solid #E2E8F0':'none',transition:'background 0.2s'}}>
                  <input ref={ref} type="file" accept=".csv" style={{display:'none'}}
                    onChange={e=>{uploadTable(t.name,e.target.files[0]);e.target.value='';}}/>
                  <button onClick={()=>!isUp&&ref.current&&ref.current.click()} disabled={isUp}
                    style={{display:'flex',alignItems:'center',justifyContent:'center',width:28,height:28,
                      borderRadius:6,border:`1.5px solid ${s.border}`,background:s.bg,color:s.color,
                      cursor:isUp?'not-allowed':'pointer',fontWeight:700,fontSize:13,flexShrink:0}}
                    onMouseOver={e=>{if(!isUp&&tableStatus[t.name]!=='done')e.currentTarget.style.background='#D1FAE5';}}
                    onMouseOut={e=>{e.currentTarget.style.background=s.bg;}}>
                    {isUp?<span style={{animation:'spin 1s linear infinite',display:'inline-block'}}>↻</span>:s.icon}
                  </button>
                  <div style={{minWidth:52,fontFamily:'monospace',fontWeight:700,fontSize:13,color:'#006B3C',
                    background:'#EDFAF4',padding:'3px 8px',borderRadius:4,textAlign:'center',flexShrink:0}}>{t.name}</div>
                  <div style={{fontSize:13,color:'#475569',flex:1}}>{t.desc}</div>
                  {tableMsg[t.name]&&<div style={{fontSize:11,color:tableStatus[t.name]==='error'?'#D13438':'#107C10',fontWeight:600,whiteSpace:'nowrap'}}>{tableMsg[t.name]}</div>}
                </div>
              );
            })}
          </div>
          <div style={{marginTop:16,display:'flex',alignItems:'center',gap:12,justifyContent:'space-between',flexWrap:'wrap'}}>
            <div style={{fontSize:12,color:allDone?'#107C10':'#94a3b8',fontWeight:allDone?700:400}}>
              {allDone?'✓ All 9 tables uploaded — ready to build':`${tables.filter(t=>tableStatus[t.name]==='done').length} / ${tables.length} tables uploaded`}
            </div>
            <div style={{display:'flex',alignItems:'center',gap:10,flexWrap:'wrap'}}>
              {buildMsg&&<div style={{fontSize:12,color:buildMsg.startsWith('Error')?'#D13438':'#107C10',fontWeight:600}}>{buildMsg}</div>}
              {buildMsg&&!buildMsg.startsWith('Error')&&(
                <button onClick={()=>{const url=`${API}/o2c/download_output?username=${encodeURIComponent(currentUser||'Unknown')}`;const a=document.createElement('a');a.href=url;a.download='';a.click();}}
                  style={{background:'#006B3C',color:'#fff',border:'none',padding:'5px 14px',borderRadius:4,fontSize:12,fontWeight:700,cursor:'pointer'}}
                  onMouseOver={e=>e.currentTarget.style.background='#004d2c'}
                  onMouseOut={e=>e.currentTarget.style.background='#006B3C'}>
                  ⬇ Download CSV
                </button>
              )}
              <button onClick={handleBuild} disabled={!allDone||anyUploading}
                style={{background:allDone&&!anyUploading?'#006B3C':'#A8A8A8',color:'#fff',border:'none',
                  padding:'10px 28px',borderRadius:6,fontSize:13,fontWeight:700,
                  cursor:allDone&&!anyUploading?'pointer':'not-allowed',whiteSpace:'nowrap',
                  boxShadow:allDone&&!anyUploading?'0 2px 8px rgba(0,107,60,0.3)':'none'}}
                onMouseOver={e=>{if(allDone&&!anyUploading)e.currentTarget.style.background='#004d2c';}}
                onMouseOut={e=>{e.currentTarget.style.background=allDone&&!anyUploading?'#006B3C':'#A8A8A8';}}>
                {building?'⏳ Building…':'⚡ Build Event Log'}
              </button>
            </div>
          </div>
        </div>

        {/* Previous Table Builds */}
        <div style={{background:'#fff',border:'1px solid #E2E8F0',borderRadius:10,padding:'18px 20px',boxShadow:'0 2px 6px rgba(0,0,0,0.04)'}}>
          <div style={{fontSize:11,fontWeight:700,color:'#64748b',textTransform:'uppercase',letterSpacing:0.8,marginBottom:12}}>Previous Table Builds</div>
          {fetchingFiles?(
            <div style={{color:'#94a3b8',fontSize:13}}>Loading...</div>
          ):tableBuilds.length===0?(
            <div style={{padding:'20px',textAlign:'center',background:'#F8FAFC',borderRadius:8,border:'1px dashed #E2E8F0',color:'#94a3b8',fontSize:13}}>
              No previous builds found. Upload tables above and click Build.
            </div>
          ):(
            <div style={{border:'1px solid #E2E8F0',borderRadius:8,overflow:'hidden'}}>
              <table style={{width:'100%',borderCollapse:'collapse',fontSize:12}}>
                <thead style={{background:'#F3F2F1',borderBottom:'1px solid #E2E8F0',textAlign:'left'}}>
                  <tr>
                    <th style={{padding:'9px 14px',color:'#323130',fontWeight:600}}>Name</th>
                    <th style={{padding:'9px 14px',color:'#323130',fontWeight:600}}>Date</th>
                    <th style={{padding:'9px 14px',color:'#323130',fontWeight:600,textAlign:'right'}}>Cases</th>
                    <th style={{padding:'9px 14px',color:'#323130',fontWeight:600,textAlign:'right'}}>Rows</th>
                    <th style={{padding:'9px 14px',color:'#323130',fontWeight:600}}>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {tableBuilds.map((f,idx)=>(
                    <tr key={idx} style={{borderBottom:'1px solid #E2E8F0',transition:'background 0.2s'}}
                      onMouseEnter={e=>e.currentTarget.style.background='#F8FAFC'}
                      onMouseLeave={e=>e.currentTarget.style.background='transparent'}>
                      <td style={{padding:'9px 14px'}}>
                        <div style={{fontWeight:600,color:'#1e293b',fontSize:12}}>{f.filename}</div>
                        <span style={{fontSize:9,fontWeight:700,padding:'2px 7px',borderRadius:10,background:'#EDFAF4',color:'#006B3C',border:'1px solid #A8D5B5'}}>Table Build</span>
                      </td>
                      <td style={{padding:'9px 14px',color:'#64748b',whiteSpace:'nowrap',fontSize:11}}>{f.upload_date}</td>
                      <td style={{padding:'9px 14px',color:'#1e293b',fontWeight:600,textAlign:'right'}}>{f.cases!=null?Number(f.cases).toLocaleString():'—'}</td>
                      <td style={{padding:'9px 14px',color:'#64748b',textAlign:'right'}}>{f.rows!=null?Number(f.rows).toLocaleString():'—'}</td>
                      <td style={{padding:'9px 14px'}}>
                        <button onClick={()=>handleLoadOldFile&&handleLoadOldFile(f.file_id)}
                          style={{background:'#006B3C',color:'#fff',border:'none',padding:'5px 12px',borderRadius:4,cursor:'pointer',fontSize:11,fontWeight:600}}
                          onMouseOver={e=>e.currentTarget.style.background='#004d2c'}
                          onMouseOut={e=>e.currentTarget.style.background='#006B3C'}>
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

const UploadBanner=({onUploaded,serverOk,onLoadingChange,currentUser,myFiles,fetchingFiles,handleLoadOldFile})=>{
  const [step,setStep]=useState('info');
  const [dragging,setDragging]=useState(false);
  const [status,setStatus]=useState('idle');
  const [msg,setMsg]=useState('');
  const inputRef=useRef();

  const doUpload=async(file)=>{
    if(!file) return;
    if(!file.name.toLowerCase().endsWith('.csv')){setStatus('error');setMsg('Only .csv files accepted.');return;}
    setStatus('uploading');setMsg('');
    onLoadingChange(true,10,'Processing Data...');
    const form=new FormData();
    form.append('file',file); form.append('username',currentUser);
    let prog=10;
    const ticker=setInterval(()=>{prog=Math.min(prog+Math.random()*12,88);onLoadingChange(true,prog,'Analysing Data...');},400);
    try{
      const r=await fetch(`${API}/o2c/upload`,{method:'POST',body:form});
      const d=await r.json(); clearInterval(ticker);
      if(!r.ok) throw new Error(d.detail||`HTTP ${r.status}`);
      onLoadingChange(true,100,'Dashboard Created');
      setTimeout(()=>{ setStatus('done'); setMsg(`✓ ${Number(d.rows).toLocaleString()} rows · ${Number(d.unique_cases).toLocaleString()} unique cases`); onUploaded(); setTimeout(()=>{ onLoadingChange(false,0,''); }, 500); },800);
    }catch(e){
      clearInterval(ticker);onLoadingChange(false,0,'');setStatus('error');setMsg(`Error: ${e.message}`);
    }finally{
      if(inputRef.current) inputRef.current.value='';
    }
  };

  /* Page 1: Info/landing */
  if(step==='info') return(
    <O2CIntroScreen onGoTableBuild={()=>setStep('table')} onGoCsvUpload={()=>setStep('upload')} />
  );

  /* Page 2: Table upload → Build Event Log */
  if(step==='table') return(
    <O2CTableUploadScreen
      onBuilt={onUploaded}
      onBack={()=>setStep('info')}
      onLoadingChange={onLoadingChange}
      currentUser={currentUser}
      myFiles={myFiles}
      fetchingFiles={fetchingFiles}
      handleLoadOldFile={handleLoadOldFile}
    />
  );

  /* Page 3: Pre-built CSV upload */
  if(step==='upload'){
    const bc=dragging?C.blue700:status==='done'?'#107C10':status==='error'?C.red:C.border;
    const bg=dragging?'#E8F5EE':status==='done'?'#F0FAF0':status==='error'?'#FDE7E9':'#FAFAFA';
    const SCHEMA_COLS=[
      {col:'Subsequent Document',       desc:'Case key (VBELN+POSNN from VBFA)',                 req:true},
      {col:'Activity',                  desc:'Activity name (after Unpivot+Renamer)',             req:true},
      {col:'Timestamp',                 desc:'Activity timestamp (after Unpivot+Renamer)',        req:true},
      {col:'Sales Order Number',        desc:'VBELN from VBAK',                                  req:false},
      {col:'Sales Document Creation Date',desc:'ERDAT from VBAK',                               req:false},
      {col:'Sales Document Maker',      desc:'ERNAM from VBAK',                                  req:false},
      {col:'Sales Document Type',       desc:'AUART from VBAK',                                  req:false},
      {col:'Delivery Block',            desc:'LIFSK from VBAK',                                  req:false},
      {col:'Billing Block',             desc:'FAKSK from VBAK',                                  req:false},
      {col:'Delivery Blocked Date',     desc:'Rule: LIFSK not blank → Header Changed Date',      req:false},
      {col:'Billing Block Date',        desc:'Rule: FAKSK not blank → Header Changed Date',      req:false},
      {col:'Delivery Creation Date',    desc:'ERDAT from VBFA where VBTYP_N=J',                  req:false},
      {col:'Goods Movement Date',       desc:'ERDAT from VBFA where VBTYP_N=R',                  req:false},
      {col:'GI Reversed',               desc:'ERDAT from VBFA where VBTYP_N=H',                  req:false},
      {col:'Invoice Creation Date',     desc:'ERDAT from VBFA where VBTYP_N=M',                  req:false},
      {col:'Invoice Reversal Date',     desc:'ERDAT from VBFA where VBTYP_N=N',                  req:false},
      {col:'Credit Memo Date',          desc:'ERDAT from VBFA where VBTYP_N=P',                  req:false},
      {col:'Debit Memo Date',           desc:'ERDAT from VBFA where VBTYP_N=O',                  req:false},
      {col:'Clearing Date',             desc:'AUGDT from BSAD',                                  req:false},
      {col:'Goods Issued',              desc:'WADAT_IST from LIKP',                              req:false},
      {col:'VKORG',                     desc:'Sales Organisation',                               req:false},
      {col:'NAME1',                     desc:'Customer name (from KNA1)',                        req:false},
      {col:'MATNR',                     desc:'Material number',                                  req:false},
      {col:'WERKS',                     desc:'Plant',                                            req:false},
    ];
    const csvUploads=(myFiles||[]).filter(f=>!f.source||f.source==='csv_upload');
    return(
      <div style={{display:'flex',flexDirection:'column',gap:16,padding:'20px 14px'}}>
        <div style={{display:'flex',alignItems:'center',gap:10}}>
          <button onClick={()=>setStep('info')}
            style={{background:'none',border:'1px solid #E2E8F0',padding:'5px 12px',borderRadius:6,
              fontSize:12,cursor:'pointer',color:'#64748b',fontWeight:600,flexShrink:0}}
            onMouseOver={e=>e.currentTarget.style.background='#F8FAFC'}
            onMouseOut={e=>e.currentTarget.style.background='none'}>
            ← Back
          </button>
          <div style={{fontSize:11,fontWeight:700,color:'#0078D4',textTransform:'uppercase',letterSpacing:0.8}}>Upload Pre-built CSV</div>
          <div style={{display:'flex',alignItems:'center',gap:5,fontSize:11,color:C.slate,marginLeft:'auto'}}>
            <div style={{width:7,height:7,borderRadius:'50%',background:serverOk?'#107C10':'#D13438',
              boxShadow:serverOk?'0 0 0 2px rgba(16,124,16,.2)':'0 0 0 2px rgba(209,52,56,.2)'}}/>
            {serverOk?'Backend connected':'Backend offline'}
          </div>
        </div>

        {/* Schema panel */}
        <div style={{background:'#fff',border:'1px solid #E2E8F0',borderRadius:10,padding:'16px 18px',boxShadow:'0 2px 6px rgba(0,0,0,0.04)'}}>
          <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:12,flexWrap:'wrap',gap:6}}>
            <div style={{fontSize:11,fontWeight:700,color:'#64748b',textTransform:'uppercase',letterSpacing:0.8}}>Expected CSV Columns</div>
            <div style={{fontSize:11,color:'#94a3b8'}}>Dates in <strong style={{color:'#475569'}}>YYYY-MM-DD</strong></div>
          </div>
          <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:4}}>
            {SCHEMA_COLS.map(({col,desc,req})=>(
              <div key={col} style={{display:'flex',alignItems:'flex-start',gap:7,padding:'5px 9px',borderRadius:5,
                background:req?'#F0FBF4':'#F8FAFC',border:`1px solid ${req?'#B7E4C7':'#E2E8F0'}`}}>
                <span style={{fontSize:9,fontWeight:800,padding:'2px 5px',borderRadius:8,marginTop:2,
                  whiteSpace:'nowrap',flexShrink:0,background:req?'#006B3C':'#94a3b8',color:'#fff'}}>
                  {req?'REQ':'OPT'}
                </span>
                <div>
                  <div style={{fontSize:11,fontWeight:700,color:'#1e293b',fontFamily:'monospace'}}>{col}</div>
                  <div style={{fontSize:10,color:'#64748b',lineHeight:1.3}}>{desc}</div>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Drop zone */}
        <div onDragOver={e=>{e.preventDefault();setDragging(true);}}
          onDragLeave={()=>setDragging(false)}
          onDrop={e=>{e.preventDefault();setDragging(false);doUpload(e.dataTransfer.files[0]);}}
          onClick={()=>{if(status!=='uploading'&&inputRef.current){inputRef.current.value='';inputRef.current.click();}}}
          style={{border:`2px dashed ${bc}`,borderRadius:8,padding:'14px 24px',background:bg,
            cursor:'pointer',textAlign:'center',transition:'all .2s',
            display:'flex',alignItems:'center',justifyContent:'center',gap:14}}>
          <input ref={inputRef} type="file" accept=".csv" style={{display:'none'}} onChange={e=>doUpload(e.target.files[0])}/>
          <div style={{fontSize:22,fontWeight:'bold',color:status==='done'?'#107C10':status==='error'?'#D13438':'#006B3C'}}>
            {status==='done'?'✓':status==='error'?'✕':'⬆'}
          </div>
          <div style={{textAlign:'left'}}>
            <div style={{fontSize:13,fontWeight:700,color:'#323130'}}>
              {status==='idle'?'Click or drag & drop a CSV file here':status==='done'?'File loaded!':'Upload failed'}
            </div>
            <div style={{fontSize:11,color:C.slate,marginTop:2}}>{msg||'Wide-format O2C event log CSV'}</div>
          </div>
          {status==='done'&&(
            <button onClick={e=>{e.stopPropagation();setStatus('idle');setMsg('');}}
              style={{fontSize:11,padding:'4px 10px',background:'#fff',marginLeft:8,
                border:`1px solid ${C.border}`,borderRadius:4,cursor:'pointer',color:C.slate}}>
              Replace
            </button>
          )}
        </div>

        {/* Previous CSV Uploads */}
        <div style={{background:'#fff',border:`1px solid ${C.border}`,borderRadius:10,padding:'18px 20px',boxShadow:'0 2px 6px rgba(0,0,0,0.04)'}}>
          <div style={{fontSize:11,fontWeight:700,color:'#64748b',textTransform:'uppercase',letterSpacing:0.8,marginBottom:12}}>Previous CSV Uploads</div>
          {fetchingFiles?(
            <div style={{color:'#94a3b8',fontSize:13}}>Loading...</div>
          ):csvUploads.length===0?(
            <div style={{padding:'20px',textAlign:'center',background:'#F8FAFC',borderRadius:8,border:'1px dashed #E2E8F0',color:'#94a3b8',fontSize:13}}>
              No previous CSV uploads found.
            </div>
          ):(
            <div style={{border:'1px solid #E2E8F0',borderRadius:8,overflow:'hidden'}}>
              <table style={{width:'100%',borderCollapse:'collapse',fontSize:12}}>
                <thead style={{background:'#F3F2F1',borderBottom:'1px solid #E2E8F0',textAlign:'left'}}>
                  <tr>
                    <th style={{padding:'9px 14px',color:'#323130',fontWeight:600}}>File Name</th>
                    <th style={{padding:'9px 14px',color:'#323130',fontWeight:600}}>Date</th>
                    <th style={{padding:'9px 14px',color:'#323130',fontWeight:600,textAlign:'right'}}>Cases</th>
                    <th style={{padding:'9px 14px',color:'#323130',fontWeight:600,textAlign:'right'}}>Rows</th>
                    <th style={{padding:'9px 14px',color:'#323130',fontWeight:600}}>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {csvUploads.map((f,idx)=>(
                    <tr key={idx} style={{borderBottom:'1px solid #E2E8F0',transition:'background 0.2s'}}
                      onMouseEnter={e=>e.currentTarget.style.background='#F8FAFC'}
                      onMouseLeave={e=>e.currentTarget.style.background='transparent'}>
                      <td style={{padding:'9px 14px'}}>
                        <div style={{fontWeight:600,color:'#1e293b',fontSize:12,wordBreak:'break-all'}}>{f.filename}</div>
                        <span style={{fontSize:9,fontWeight:700,padding:'2px 7px',borderRadius:10,background:'#EFF6FF',color:'#0057B7',border:'1px solid #B3D1F5'}}>CSV Upload</span>
                      </td>
                      <td style={{padding:'9px 14px',color:'#64748b',whiteSpace:'nowrap',fontSize:11}}>{f.upload_date}</td>
                      <td style={{padding:'9px 14px',color:'#1e293b',fontWeight:600,textAlign:'right'}}>{f.cases!=null?Number(f.cases).toLocaleString():'—'}</td>
                      <td style={{padding:'9px 14px',color:'#64748b',textAlign:'right'}}>{f.rows!=null?Number(f.rows).toLocaleString():'—'}</td>
                      <td style={{padding:'9px 14px'}}>
                        <button onClick={()=>handleLoadOldFile(f.file_id)}
                          style={{background:'#0078D4',color:'#fff',border:'none',padding:'5px 12px',borderRadius:4,cursor:'pointer',fontSize:11,fontWeight:600}}
                          onMouseOver={e=>e.currentTarget.style.background='#005A9E'}
                          onMouseOut={e=>e.currentTarget.style.background='#0078D4'}>
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
};

/* ════════════════════════════════════════════
   MAIN APP
════════════════════════════════════════════ */
export default function O2CDashboard({ currentUser, onSignOut, onBackHome }){
  const [serverOk,  setServerOk]  =useState(false);
  const [dataLoaded,setDataLoaded]=useState(false);
  const [loading,   setLoading]   =useState(false); 
  const [dashboardLoading, setDashboardLoading] = useState(false); 
  
  const [chartsReady, setChartsReady] = useState(false);
  const [pmReady, setPmReady] = useState(false);
  
  const intentToUpload = useRef(true); 
  
  const [loadProg,  setLoadProg]  =useState(0);
  const [loadLabel, setLoadLabel] =useState('');
  const [filters,   setFilters]   =useState({});
  
  const [activeTab, setActiveTab] = useState('process');
  const [layoutDir, setLayoutDir] = useState('TB');

  const [selected,  setSelected]  =useState({
    case_id:'ALL',customer:'ALL',vkorg:'ALL',auart:'ALL',matkl:'ALL',werks:'ALL',
    year:'ALL',quarter:'ALL',month:'ALL',ernam:'ALL',status:'ALL',lead_time:'ALL'
  });
  const [crossFilter,setCrossFilter]=useState(null);
  const [hoverInfo,  setHoverInfo]  =useState(null);
  
  const [kpis,      setKpis]      =useState(null);
  const [actData,   setActData]   =useState([]);
  const [monData,   setMonData]   =useState([]);
  const [custData,  setCustData]  =useState([]);
  const [auartData, setAuartData] =useState([]);
  const [matklData, setMatklData] =useState([]);
  const [vkorgData, setVkorgData] =useState([]);
  const [ltData,    setLtData]    =useState([]);
  const [ernamData, setErnamData] =useState([]); 
  const [caseTableData, setCaseTableData] = useState([]); 
  const [caseEvents, setCaseEvents] = useState([]); 

  const [invRevErnam, setInvRevErnam] = useState([]);
  const [invRevTimeline, setInvRevTimeline] = useState([]);
  const [seqViolation, setSeqViolation] = useState([]);
  const [happyPathData, setHappyPathData] = useState([]);
  const [deviationsSummary, setDeviationsSummary] = useState([]);
  const [sodData, setSodData] = useState([]);
  
  const [bottleneckData, setBottleneckData] = useState([]);
  const [custLeadTime, setCustLeadTime] = useState([]);

  const [pmLoading, setPmLoading] =useState(false);
  const [pmError,   setPmError]   =useState('');
  
  const [rfNodes,setRfNodes,onNodesChange]=useNodesState([]);
  const [rfEdges,setRfEdges,onEdgesChange]=useEdgesState([]);
  const [rawGraphData, setRawGraphData] = useState(null);

  const [refreshTrigger, setRefreshTrigger] = useState(0);

  const [myFiles, setMyFiles] = useState([]);
  const [fetchingFiles, setFetchingFiles] = useState(false);

  useEffect(() => {
    if (currentUser && !dataLoaded) {
      setFetchingFiles(true);
      fetch(`${API}/o2c/my_files?username=${currentUser}`)
        .then(res => res.ok ? res.json() : [])
        .then(data => setMyFiles(data))
        .catch(err => console.error("Failed to fetch files", err))
        .finally(() => setFetchingFiles(false));
    }
  }, [currentUser, dataLoaded, refreshTrigger]);

  const handleLoadOldFile = async (file_id) => {
    handleLoadingChange(true, 50, 'Loading previous dashboard...');
    try {
      const res = await fetch(`${API}/o2c/load_file`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: currentUser, file_id })
      });
      if (!res.ok) throw new Error('Failed to load file');
      intentToUpload.current = false;
      setDataLoaded(true);
      handleRefresh();
    } catch(e) {
      alert("Error loading dashboard: " + e.message);
    } finally {
      setTimeout(() => handleLoadingChange(false, 100, ''), 500);
    }
  };

  const logAction = useCallback((action, details) => {
    if (!currentUser) return;
    fetch(`${API}/o2c/log`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: currentUser, action, details })
    }).catch(e => console.error("Logging failed", e));
  }, [currentUser]);

  const handleLoadingChange=useCallback((vis,prog,lbl)=>{
    setLoading(vis);setLoadProg(prog);setLoadLabel(lbl);
  },[]);

  const handleSignOut = async () => {
    logAction('LOGOUT', 'User signed out');
    try { await fetch(`${API}/o2c/clear?username=${encodeURIComponent(currentUser||'Unknown')}`, { method: 'POST' }); } catch(e){}
    intentToUpload.current = true;
    setDataLoaded(false);
    setChartsReady(false);
    setPmReady(false);
    setLoading(false);
    setLoadProg(0);
    setLoadLabel('');
    setKpis(null);
    if (onSignOut) onSignOut();
  };

  const handleResetData = () => {
    logAction('RESET_DATA', 'Started upload new file flow');
    intentToUpload.current = true;
    setDataLoaded(false);
    setChartsReady(false);
    setPmReady(false);
    setLoading(false);
    setLoadProg(0);
    setLoadLabel('');
    setKpis(null);
    setActData([]);
    setCaseTableData([]);
    setCaseEvents([]);
    setErnamData([]);
    setSelected({
      case_id:'ALL',customer:'ALL',vkorg:'ALL',auart:'ALL',matkl:'ALL',werks:'ALL',
      year:'ALL',quarter:'ALL',month:'ALL',ernam:'ALL',status:'ALL',lead_time:'ALL'
    });
    setCrossFilter(null);
  };
  
  const handleRefresh = () => {
    logAction('REFRESH', 'Refreshed the dashboard');
    setRefreshTrigger(p => p + 1);
  };

  useEffect(()=>{
    if (!currentUser) return;
    const ping=()=>fetch(`${API}/o2c/?username=${encodeURIComponent(currentUser||'Unknown')}`).then(r=>r.ok?r.json():null)
      .then(d=>{
        setServerOk(!!(d?.status));
        if(d?.data_loaded && !intentToUpload.current) {
           setDataLoaded(prev => {
              if(!prev) {
                 setLoading(true);
                 setLoadProg(100);
                 setLoadLabel('Loading existing dashboard...');
                 return true;
              }
              return prev;
           });
        }
      }).catch(()=>{
          setServerOk(false);
      });
    ping();const t=setInterval(ping,5000);return()=>clearInterval(t);
  },[currentUser]);

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

  const handleSelect=useCallback((type,value)=>{
    setCrossFilter(prev=>{
       const isRemoving = prev?.type === type && prev?.value === value;
       if(isRemoving) logAction('FILTER', `Cleared cross-filter on chart: ${type}`);
       else logAction('FILTER', `Applied cross-filter on chart: ${type} = ${value}`);
       return isRemoving ? null : {type,value};
    });
  },[logAction]);
  
  const clearCF=useCallback(()=>{
    logAction('FILTER', 'Cleared all active chart cross-filters');
    setCrossFilter(null);
  },[logAction]);

  useEffect(()=>{
    if(!dataLoaded) return;
    fetch(`${API}/o2c/filters${baseQStr()}`)
      .then(r=>r.ok?r.json():{})
      .then(d=>setFilters(d&&typeof d==='object'&&!Array.isArray(d)?d:{}))
      .catch(()=>setFilters({}));
  },[baseQStr,dataLoaded, refreshTrigger]);

  useEffect(() => {
    if (!dataLoaded) return;
    if (selected.case_id !== 'ALL' && selected.case_id != null) {
       fetch(`${API}/o2c/case_events?case_id=${encodeURIComponent(selected.case_id)}&username=${encodeURIComponent(currentUser || 'Unknown')}`)
          .then(r => r.ok ? r.json() : [])
          .then(setCaseEvents)
          .catch(() => setCaseEvents([]));
    } else {
       setCaseEvents([]);
    }
  }, [selected.case_id, dataLoaded, refreshTrigger, currentUser]);

  useEffect(()=>{
    if(!dataLoaded) return;
    if(chartsReady) setDashboardLoading(true);

    const cq = effectiveQStr();

    // Timeout wrapper — every fetch resolves within 60s max (never hangs Promise.all)
    const ft = (url, ms=60000) => Promise.race([
      fetch(url),
      new Promise((_,rej) => setTimeout(()=>rej(new Error('timeout')), ms))
    ]);
    const arr = (u,s) => ft(u).then(r=>r.ok?r.json():[]).then(d=>s(Array.isArray(d)?d:[])).catch(()=>s([]));
    const obj = (u,s) => ft(u).then(r=>r.ok?r.json():null).then(d=>s(d&&typeof d==='object'&&!Array.isArray(d)?d:null)).catch(()=>s(null));

    const chartPromises = [
      obj(`${API}/o2c/kpis${cq}`,                      setKpis),
      arr(`${API}/o2c/charts/activity${cq}`,            setActData),
      arr(`${API}/o2c/charts/monthly${cq}`,             setMonData),
      arr(`${API}/o2c/charts/customer${cq}`,            setCustData),
      arr(`${API}/o2c/charts/auart${cq}`,               setAuartData),
      arr(`${API}/o2c/charts/matkl${cq}`,               setMatklData),
      arr(`${API}/o2c/charts/ernam${cq}`,               setErnamData),
      arr(`${API}/o2c/charts/vkorg${cq}`,               setVkorgData),
      arr(`${API}/o2c/charts/leadtime${cq}`,            setLtData),
      arr(`${API}/o2c/charts/bottleneck${cq}`,          setBottleneckData),
      arr(`${API}/o2c/charts/sod${cq}`,                 setSodData),
      arr(`${API}/o2c/charts/inv_rev_ernam${cq}`,       setInvRevErnam),
      arr(`${API}/o2c/charts/inv_rev_timeline${cq}`,    setInvRevTimeline),
      arr(`${API}/o2c/charts/customer_lead_time${cq}`,  setCustLeadTime),
      arr(`${API}/o2c/charts/seq_violation_ernam${cq}`, setSeqViolation),
      arr(`${API}/o2c/charts/happy_path${cq}`,          setHappyPathData),
      arr(`${API}/o2c/charts/deviations_summary${cq}`,  setDeviationsSummary),
      arr(`${API}/o2c/cases${cq}`,                      setCaseTableData),
    ];

    // Process-map runs in parallel — always resolves (errors are caught)
    const pmPromise = ft(`${API}/o2c/process-map${cq}`, 60000)
      .then(r => { if(!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then(d => {
        setRawGraphData(d);
        buildFlowMap(d.nodes, d.edges, setRfNodes, setRfEdges, layoutDir);
        setPmReady(true);
      })
      .catch(err => { setPmError(`Failed: ${err.message}`); setPmReady(true); })
      .finally(() => { setPmLoading(false); });

    // Single Promise.all — clears the loading overlay only when EVERYTHING is done
    Promise.all([...chartPromises, pmPromise]).finally(() => {
      setDashboardLoading(false);
      setChartsReady(true);
      setLoading(false);
      setLoadProg(0);
      setLoadLabel('');
    });

  },[effectiveQStr, dataLoaded, refreshTrigger]);

  useEffect(() => {
    if (rawGraphData) {
      buildFlowMap(rawGraphData.nodes, rawGraphData.edges, setRfNodes, setRfEdges, layoutDir);
    }
  }, [layoutDir, rawGraphData]);

  const slicer=(key,label,filterKey,wide=false)=>{
    const raw=filters[filterKey];
    const opts=Array.isArray(raw)?raw:['ALL'];
    const deduped=opts[0]==='ALL'?opts:['ALL',...opts];
    
    const handleSlicerChange = (val) => {
        logAction('FILTER', `Changed slicer ${key} to ${val}`);
        setSelected(prev=>({...prev,[key]:val}));
        setCrossFilter(null);
    };

    if(key === 'case_id' || key === 'customer' || key === 'vkorg') {
        return (
            <SearchableSelect key={key} label={label} value={selected[key]||'ALL'}
                options={deduped} wide={wide}
                onChange={handleSlicerChange}/>
        );
    }

    return(
      <FilterSelect key={key} label={label} value={selected[key]||'ALL'}
        options={deduped} wide={wide}
        onChange={handleSlicerChange}/>
    );
  };

  const resetAll=()=>{
    logAction('FILTER', 'Reset all slicers to ALL');
    setSelected({
      case_id:'ALL',customer:'ALL',vkorg:'ALL',auart:'ALL',matkl:'ALL',werks:'ALL',
      year:'ALL',quarter:'ALL',month:'ALL',ernam:'ALL',status:'ALL',lead_time:'ALL'
    });
    setCrossFilter(null);
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

  const kpiTooltips={
    total_cases:        'Total unique Sales Orders (cases) in dataset',
    avg_cycle_days:     'Average Order to Cash complete cycle time',
    so_approved:        'Cases where SO was approved (no delivery/billing block)',
    deliveries_created: 'Cases with a Delivery document created',
    deliveries_posted:  'Cases with Delivery document posted (WADAT)',
    goods_issues:       'Cases with Goods Issue posted',
    invoices_created:   'Cases with an Invoice document created',
    invoices_posted:    'Cases with Invoice posted to Accounting',
    invoices_cleared:   'Cases with Invoice fully cleared / payment received',
    so_reversals:       'Sales Orders reversed / rejected',
    so_rev_after_gi:    'Sales Orders reversed AFTER Goods Issue (high risk)',
    gi_reversals:       'Goods Issue documents reversed / cancelled',
    invoice_reversals:  'Invoice reversals posted',
    credit_memos:       'Credit memos issued',
    debit_memos:        'Debit memos issued',
    inv_no_del:         'Invoices raised without a Delivery document',
    inv_no_gi:          'Invoices raised before Goods Issue',
  };

  return(
    <div style={{fontFamily:"'Segoe UI',-apple-system,sans-serif",
      background:C.bg,height:'100vh',display:'flex',flexDirection:'column'}}>

      <style>{`
        @keyframes spin{to{transform:rotate(360deg)}}
        *{box-sizing:border-box}
        ::-webkit-scrollbar{width:5px;height:5px}
        ::-webkit-scrollbar-track{background:transparent}
        ::-webkit-scrollbar-thumb{background:#D2D0CE;border-radius:3px}
        ::-webkit-scrollbar-thumb:hover{background:#A19F9D}
      `}</style>

      <LoadingOverlay visible={loading} progress={loadProg} label={loadLabel}/>

      <div style={{background:C.headerBg,padding:'10px 20px',flexShrink:0,
        display:'flex',justifyContent:'space-between',alignItems:'center',
        boxShadow:'0 2px 8px rgba(0,0,0,.2)'}}>
        <div style={{display:'flex',alignItems:'center',gap:12}}>
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
            <div style={{fontWeight:700,fontSize:16,color:'#fff'}}>O2C Process Explorer</div>
            <div style={{fontSize:10,color:'rgba(255,255,255,.5)'}}>Order-to-Cash Process Mining</div>
          </div>
          {crossFilter&&(
            <div style={{display:'flex',alignItems:'center',gap:6,marginLeft:16,
              background:'rgba(255,255,255,.12)',border:'1px solid rgba(255,255,255,.2)',
              borderRadius:6,padding:'4px 12px',fontSize:12}}>
              <span style={{color:'#fff',fontWeight:600}}>Filter: {crossFilter.type}: <strong>{crossFilter.value}</strong></span>
              <button onClick={clearCF} style={{background:'none',border:'none',cursor:'pointer',
                color:'rgba(255,255,255,.8)',fontWeight:700,fontSize:14,padding:'0 2px'}}>✕</button>
            </div>
          )}
        </div>
        
        <div style={{display:'flex', alignItems:'center', gap:12}}>
          {dataLoaded&&kpis&&(
            <div style={{fontSize:11,color:'rgba(255,255,255,.5)'}}>
              {Number(kpis.total_cases).toLocaleString()} order{Number(kpis.total_cases) === 1 ? '' : 's'} loaded
            </div>
          )}

          {dataLoaded && (
            <div style={{display:'flex', alignItems:'center', gap:4,
              background:'rgba(255,255,255,0.08)', borderRadius: '24px',
              padding:'4px', border:'1px solid rgba(255,255,255,0.1)'}}>
              <button
                style={getTabStyle(activeTab === 'process')}
                onClick={() => { logAction('TAB', 'Viewed Process Mining'); setActiveTab('process'); }}
              >
                Process Mining
              </button>
              <button
                style={getTabStyle(activeTab === 'dimensions')}
                onClick={() => { logAction('TAB', 'Viewed Dimensions'); setActiveTab('dimensions'); }}
              >
                EDA
              </button>
              <button
                style={{...getTabStyle(false), color: '#C8E6DA'}}
                onClick={handleResetData}
                onMouseOver={e => { e.currentTarget.style.background = 'rgba(255,255,255,0.1)'; }}
                onMouseOut={e => { e.currentTarget.style.background = 'transparent'; }}
              >
                Upload New File
              </button>
            </div>
          )}

          <div style={{display: 'flex', alignItems: 'center', gap: 12, marginLeft: 16}}>
             <div style={{width: 1, height: 24, background: 'rgba(255,255,255,0.2)'}}></div>
             <div style={{fontSize: 12, color: 'rgba(255,255,255,0.7)'}}>
                User: <strong style={{color: '#fff'}}>{currentUser}</strong>
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

      <div style={{flex:1,overflowY:'auto',padding:'12px 14px 40px',
        display:'flex',flexDirection:'column',gap:10}}>

        {!dataLoaded && (
             <UploadBanner 
               currentUser={currentUser}
               onUploaded={() => {
                 intentToUpload.current = false;
                 setDataLoaded(true);
                 handleRefresh();
                 setTimeout(()=>{ handleLoadingChange(false,0,''); }, 800);
               }} 
               serverOk={serverOk} 
               onLoadingChange={handleLoadingChange}
               myFiles={myFiles}
               fetchingFiles={fetchingFiles}
               handleLoadOldFile={handleLoadOldFile}/>
        )}

        {dataLoaded && (<>
          <div style={{background:C.card,borderRadius:8,padding:'10px 14px',
            border:`1px solid ${C.border}`,boxShadow:'0 2px 6px rgba(0,0,0,.04)'}}>
            
            <div style={{
               display: 'grid', 
               gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', 
               gap: '8px', 
               alignItems: 'end'
            }}>
              {slicer('case_id',  'Sales Order',    'case_ids', true)}
              {slicer('customer', 'Customer',       'customers')}
              {slicer('vkorg',    'Sales Org',      'vkorgs')}
              {slicer('auart',    'Order Type',     'auarts')}
              {slicer('matkl',    'Material Group', 'matkls')}
              {slicer('werks',    'Plant',          'werkss')}
              {slicer('year',     'Year',           'years')}
              {slicer('status',   'Status',         'statuses')}
              <div style={{display:'flex',flexDirection:'column',gap:4,justifyContent:'flex-end'}}>
                <div style={{fontSize:11,color:'transparent',fontWeight:600,userSelect:'none'}}>.</div>
                <div style={{display:'flex',gap:4}}>
                  <button onClick={resetAll} style={{
                      padding:'0 8px',fontSize:11,fontWeight:700,flex:1,
                      background:'#F3F2F1',color:'#323130',border:'1px solid #D2D0CE',
                      borderRadius:4,cursor:'pointer',height:'26px'
                  }}>Reset</button>
                  <button onClick={handleRefresh} style={{
                      padding:'0 8px',fontSize:11,fontWeight:700,
                      background:'#0078D4',color:'#fff',border:'none',
                      borderRadius:4,cursor:'pointer',height:'26px',
                      display:'flex',alignItems:'center',justifyContent:'center'
                  }}>🔄</button>
                  <button
                    onClick={()=>{
                      const url=`${API}/o2c/download_output?username=${encodeURIComponent(currentUser||'Unknown')}`;
                      const a=document.createElement('a');a.href=url;a.download='';a.click();
                      logAction('DOWNLOAD','Downloaded O2C output CSV');
                    }}
                    title="Download CSV"
                    style={{
                      padding:'0 8px',fontSize:11,fontWeight:700,
                      background:'#006B3C',color:'#fff',border:'none',
                      borderRadius:4,cursor:'pointer',height:'26px',
                      display:'flex',alignItems:'center',gap:3,whiteSpace:'nowrap'
                    }}
                    onMouseOver={e=>e.currentTarget.style.background='#004d2c'}
                    onMouseOut={e=>e.currentTarget.style.background='#006B3C'}
                  >⬇ CSV</button>
                </div>
              </div>
            </div>
          </div>

          {/* KPI CARDS - GUARANTEED TO RENDER */}
          {kpis ? (
            <>
              {/* ── Happy Path KPIs – green border ── */}
              <div style={{display:'grid',gridTemplateColumns:'repeat(9,1fr)',gap:8}}>
                <KpiCard label="Sales Orders"       value={kpis.total_cases}        tooltip={kpiTooltips.total_cases}/>
                <KpiCard label="Avg Life Cycle"     value={`${kpis.avg_cycle_days}d`} tooltip={kpiTooltips.avg_cycle_days}/>
                <KpiCard label="SO Approved"        value={kpis.so_approved}         tooltip={kpiTooltips.so_approved}/>
                <KpiCard label="Deliveries"         value={kpis.deliveries_created}  tooltip={kpiTooltips.deliveries_created}/>
                <KpiCard label="Delivery Posted"    value={kpis.deliveries_posted}   tooltip={kpiTooltips.deliveries_posted}/>
                <KpiCard label="Goods Issued"       value={kpis.goods_issues}        tooltip={kpiTooltips.goods_issues}/>
                <KpiCard label="Invoices"           value={kpis.invoices_created}    tooltip={kpiTooltips.invoices_created}/>
                <KpiCard label="Invoice Posted"     value={kpis.invoices_posted}     tooltip={kpiTooltips.invoices_posted}/>
                <KpiCard label="Invoice Cleared"    value={kpis.invoices_cleared}    tooltip={kpiTooltips.invoices_cleared}/>
              </div>
              {/* ── Deviation KPIs – red border ── */}
              <div style={{display:'grid',gridTemplateColumns:'repeat(8,1fr)',gap:8}}>
                <ConfKpiCard label="SO Reversed"          value={kpis.so_reversals}      sub="SO Rejections"            tooltip={kpiTooltips.so_reversals}/>
                <ConfKpiCard label="SO Rev After GI"      value={kpis.so_rev_after_gi}   sub="High-risk reversal"       tooltip={kpiTooltips.so_rev_after_gi}/>
                <ConfKpiCard label="GI Reversed"          value={kpis.gi_reversals}       sub="GI Cancellations"         tooltip={kpiTooltips.gi_reversals}/>
                <ConfKpiCard label="Invoice Reversed"     value={kpis.invoice_reversals}  sub="Invoice Cancellations"    tooltip={kpiTooltips.invoice_reversals}/>
                <ConfKpiCard label="Credit Memos"         value={kpis.credit_memos}       sub="Credits issued"           tooltip={kpiTooltips.credit_memos}/>
                <ConfKpiCard label="Debit Memos"          value={kpis.debit_memos}        sub="Debits issued"            tooltip={kpiTooltips.debit_memos}/>
                <ConfKpiCard label="Invoice w/o Delivery" value={kpis.inv_no_del}         sub="Missing delivery"         tooltip={kpiTooltips.inv_no_del}/>
                <ConfKpiCard label="Invoice Before GI"    value={kpis.inv_no_gi}          sub="Sequence violation"       tooltip={kpiTooltips.inv_no_gi}/>
              </div>
            </>
          ) : (
            dashboardLoading ? (
              <div style={{padding: '16px', textAlign: 'center', background: '#fff', borderRadius: 8, color: C.blue700, fontWeight: 600, fontSize: 13, border: `1px solid ${C.border}`}}>
                Loading Performance Indicators...
              </div>
            ) : null
          )}

          {activeTab === 'process' && (
            <div style={{display:'flex', flexDirection:'column', gap:10, flex:1, paddingBottom: '20px'}}>
              <div style={{display:'grid', gridTemplateColumns:'1fr 1fr', gap:10}}>
                <div style={{display:'flex', flexDirection:'column', gap:10}}>

                  <div style={{background:C.card,borderRadius:8,border:`1px solid ${C.border}`,
                    boxShadow:'0 2px 8px rgba(0,0,0,.05)',overflow:'hidden',
                    display:'flex',flexDirection:'column', height:960}}>

                  <div style={{padding:'12px 14px 8px',borderBottom:`1px solid ${C.border}`,
                    background: C.jkBlue, 
                    display:'flex',justifyContent:'space-between',alignItems:'center',flexShrink:0}}>
                    <div>
                      <div style={{fontSize:13,fontWeight:700,color:'#fff'}}>Process Map</div>
                      <div style={{fontSize:10,color:'rgba(255,255,255,0.7)'}}>O2C Flow & Frequency Analysis</div>
                    </div>
                    <div style={{display:'flex', gap:4, background:'rgba(255,255,255,0.2)', padding:2, borderRadius:4}}>
                      <button 
                        onClick={() => setLayoutDir('LR')}
                        style={{
                          fontSize:11, padding:'4px 8px', border:'none', cursor:'pointer', borderRadius:3,
                          background: layoutDir==='LR' ? '#fff' : 'transparent',
                          color: layoutDir==='LR' ? C.jkBlue : '#fff',
                          fontWeight: layoutDir==='LR' ? 700 : 400
                        }}>Horizontal</button>
                      <button 
                        onClick={() => setLayoutDir('TB')}
                        style={{
                          fontSize:11, padding:'4px 8px', border:'none', cursor:'pointer', borderRadius:3,
                          background: layoutDir==='TB' ? '#fff' : 'transparent',
                          color: layoutDir==='TB' ? C.jkBlue : '#fff',
                          fontWeight: layoutDir==='TB' ? 700 : 400
                        }}>Vertical</button>
                    </div>
                  </div>

                  <div style={{flex:1, position:'relative'}}>
                    {pmError&&(
                      <div style={{position:'absolute',top:8,left:8,right:8,zIndex:10,
                        fontSize:11,color:'#A4262C',background:'#FDE7E9',
                        border:'1px solid #FBC5C9',borderRadius:4,padding:'6px 12px'}}>
                        Error: {pmError}
                      </div>
                    )}

                    {pmLoading&&(
                      <div style={{position:'absolute',inset:0,zIndex:20,
                        background:'rgba(248,250,255,0.88)',backdropFilter:'blur(4px)',
                        display:'flex',flexDirection:'column',
                        alignItems:'center',justifyContent:'center',gap:14,borderRadius:6}}>
                        <div style={{textAlign:'center'}}>
                          <div style={{fontSize:13,fontWeight:700,color:'#323130',marginBottom:4}}>
                            Building Process Map
                          </div>
                          <div style={{fontSize:11,color:C.slate}}>
                            Analysing transitions and paths…
                          </div>
                        </div>
                      </div>
                    )}

                    <div style={{position:'absolute',inset:0, background:'#FAFAFA'}}>
                      <ReactFlow
                        nodes={rfNodes} edges={rfEdges}
                        onNodesChange={onNodesChange} onEdgesChange={onEdgesChange}
                        nodeTypes={nodeTypes} edgeTypes={edgeTypes}
                        fitView fitViewOptions={{padding:.18}}
                        minZoom={0.1} maxZoom={4}
                        proOptions={{hideAttribution:true}}
                        onNodeMouseEnter={(e,n)=>setHoverInfo({x:e.clientX,y:e.clientY,
                          title:n.data?.label||'',value:n.data?.frequency||0})}
                        onNodeMouseLeave={()=>setHoverInfo(null)}
                        onEdgeMouseEnter={(e,ed)=>setHoverInfo({x:e.clientX,y:e.clientY,
                          title:`${ed.source} → ${ed.target}`,
                          value:ed.data?.frequency||0,isEdge:true,avgDays:ed.data?.avg_days})}
                        onEdgeMouseLeave={()=>setHoverInfo(null)}
                        defaultEdgeOptions={{type:'freqEdge'}}>
                        <Background color="#C8E6DA" gap={24} size={1} variant="dots"/>
                        <Controls showInteractive={false}
                          style={{background:'#fff',border:`1px solid ${C.border}`,borderRadius:6}}/>
                        <MiniMap zoomable pannable
                          nodeColor={C.mapNodeBg}
                          maskColor="rgba(240,244,250,.85)"
                          style={{border:`1px solid ${C.border}`,borderRadius:6}}/>
                      </ReactFlow>
                    </div>

                    {hoverInfo&&(
                      <div style={{position:'fixed',left:hoverInfo.x+16,top:hoverInfo.y+16,
                        zIndex:99999,pointerEvents:'none',
                        background:'rgba(255,255,255,.98)',border:`1px solid ${C.border}`,
                        borderRadius:6,padding:'10px 14px',
                        boxShadow:'0 4px 12px rgba(0,0,0,.15)',fontSize:12,color:'#323130',minWidth:160}}>
                        <div style={{fontWeight:700,color:'#006B3C',marginBottom:6}}>{hoverInfo.title}</div>
                        <div style={{display:'flex',justifyContent:'space-between',gap:16}}>
                          <span style={{color:C.slate}}>{hoverInfo.isEdge?'Transitions:':'Unique Cases:'}</span>
                          <strong>{Number(hoverInfo.value).toLocaleString()}</strong>
                        </div>
                        {hoverInfo.avgDays!=null&&(
                          <div style={{display:'flex',justifyContent:'space-between',gap:16,marginTop:4}}>
                            <span style={{color:C.slate}}>Avg Duration:</span>
                            <strong>{hoverInfo.avgDays}d</strong>
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                  </div>

                </div>

                <div style={{display:'flex', flexDirection:'column', gap:10, height: '100%'}}>
                  
                  <ChartCard title="Happy Path vs Deviations" loading={dashboardLoading} highlighted={crossFilter?.type==='status'} onClear={clearCF}>
                    <StatusDonutChart data={happyPathData} crossFilter={crossFilter} onSelect={handleSelect}/>
                  </ChartCard>

                  <ChartCard title="Activity Frequency"
                    loading={dashboardLoading}
                    highlighted={crossFilter?.type==='activity'} onClear={clearCF}>
                    <ActivityChart data={actData} crossFilter={crossFilter} onSelect={handleSelect}/>
                  </ChartCard>

                  <ChartCard title="O2C Bottleneck Analysis" subtitle="Average vs Median days per process step" loading={dashboardLoading}>
                    <BottleneckChart data={bottleneckData} />
                  </ChartCard>
                </div>
              </div>

              {/* --- ANOMALIES & DEVIATIONS — 2-col grid --- */}
              <div style={{marginTop: '2px'}}>
                <div style={{display:'grid', gridTemplateColumns:'repeat(2, 1fr)', gap:10}}>
                  
                  <ChartCard title="Deviations Summary" subtitle="All deviation types by case count" loading={dashboardLoading}>
                    <DeviationsSummaryChart data={deviationsSummary} />
                  </ChartCard>

                  <ChartCard title="Segregation of Duties (SoD)" subtitle="Internal control violations (same user doing multiple actions)" loading={dashboardLoading}>
                    <SodChart data={sodData} />
                  </ChartCard>

                  <ChartCard title="Invoice Reversals Timeline" subtitle="Trend of Invoice reversals over time" loading={dashboardLoading}>
                    <MonthlyChart data={invRevTimeline} crossFilter={crossFilter} onSelect={handleSelect}/>
                  </ChartCard>

                  <ChartCard title="Invoice Reversals (by User)" subtitle="Users reversing the most Invoices" loading={dashboardLoading} highlighted={crossFilter?.type==='ernam'} onClear={clearCF}>
                    <ScrollableHBarChart data={invRevErnam} dataKey="count" labelKey="ernam" color="#5aabee" crossFilter={crossFilter} crossKey="ernam" onSelect={handleSelect}/>
                  </ChartCard>

                  <ChartCard title="Invoice Before GI (By User)" subtitle="Sequence violation: Invoice posted before Goods Issue" loading={dashboardLoading} highlighted={crossFilter?.type==='ernam'} onClear={clearCF}>
                    <ScrollableHBarChart data={seqViolation} dataKey="count" labelKey="ernam" color="#CA5010" crossFilter={crossFilter} crossKey="ernam" onSelect={handleSelect}/>
                  </ChartCard>

                  <ChartCard title="SO → GI Lead Time Distribution" 
                    loading={dashboardLoading}
                    highlighted={crossFilter?.type==='lead_time'} onClear={clearCF}>
                    <LeadTimeChart data={ltData} crossFilter={crossFilter} onSelect={handleSelect}/>
                  </ChartCard>

                </div>
              </div>

              <div style={{display:'grid', gridTemplateColumns:'1fr', gap:10, marginTop: '10px'}}>
                  <ChartCard title="Case Details" subtitle="Click a Sales Order to view its chronological event log" loading={dashboardLoading}>
                    <CaseTable 
                      data={caseTableData} 
                      events={caseEvents}
                      selectedId={selected.case_id}
                      onSelect={(id) => {
                        const newId = selected.case_id === id ? 'ALL' : id;
                        setSelected(prev => ({...prev, case_id: newId}));
                        setCrossFilter(null);
                        logAction('FILTER', `Clicked case row: ${newId}`);
                      }} 
                    />
                  </ChartCard>
              </div>

            </div>
          )}

          {activeTab === 'dimensions' && (
            <div style={{display:'grid',gridTemplateColumns:'minmax(0,1fr) minmax(0,1fr)',gap:10,
              gridAutoRows:'minmax(280px, auto)', alignItems:'stretch', paddingBottom:'24px'}}>
              
              <ChartCard title="Sales Org Distribution (VKORG)" subtitle="Cases per Sales Organisation"
                loading={dashboardLoading}
                highlighted={crossFilter?.type==='vkorg'} onClear={clearCF}>
                <GenericPieChart data={vkorgData} nameKey="vkorg" dataKey="count" crossFilter={crossFilter} onSelect={handleSelect}/>
              </ChartCard>

              <ChartCard title="Monthly Trend (Unique Cases)" subtitle="Unique active cases per month"
                loading={dashboardLoading}
                highlighted={crossFilter?.type==='month'} onClear={clearCF}>
                <MonthlyChart data={monData} crossFilter={crossFilter} onSelect={handleSelect}/>
              </ChartCard>

              <ChartCard title="User Activity (Order Creator)" subtitle="Who created Sales Orders (Unique Cases)" 
                loading={dashboardLoading}
                highlighted={crossFilter?.type==='ernam'} onClear={clearCF}>
                <ErnamChart data={ernamData} crossFilter={crossFilter} onSelect={handleSelect}/>
              </ChartCard>

              <ChartCard title="Order Type (AUART)" subtitle="Distribution by order document type"
                loading={dashboardLoading}
                highlighted={crossFilter?.type==='auart'} onClear={clearCF}>
                <ScrollableVBarChart data={auartData} crossFilter={crossFilter} onSelect={handleSelect} dataKey="count" labelKey="auart"/>
              </ChartCard>

              <ChartCard title="Material Group (MATKL)" subtitle="Sales activity by material category"
                loading={dashboardLoading}
                highlighted={crossFilter?.type==='matkl'} onClear={clearCF}>
                <ScrollableHBarChart data={matklData} dataKey="count" labelKey="matkl"
                  color="#038387" crossFilter={crossFilter} crossKey="matkl" onSelect={handleSelect}/>
              </ChartCard>

              <ChartCard title="Top Customers by Volume" subtitle="Customers by number of Sales Orders"
                loading={dashboardLoading}
                highlighted={crossFilter?.type==='customer'} onClear={clearCF}>
                <ScrollableHBarChart data={custData} dataKey="count" labelKey="customer"
                  color="#5C2D91" crossFilter={crossFilter} crossKey="customer" onSelect={handleSelect}/>
              </ChartCard>

              <ChartCard title="Avg SO → Cleared Days by Customer" subtitle="End-to-end cycle time per customer — click to filter"
                loading={dashboardLoading}
                highlighted={crossFilter?.type==='customer'} onClear={clearCF}
                style={{ gridColumn: '1 / -1' }}> 
                <CustomerAvgDaysChart data={custLeadTime} crossFilter={crossFilter} onSelect={handleSelect} />
              </ChartCard>

            </div>
          )}

        </>)}

        {/* --- NEW FILE HUB UI INSTEAD OF "NO DATA LOADED" --- */}
              </div>

      <div style={{ textAlign: 'center', fontSize: '12px', color: '#605E5C', padding: '10px 0', borderTop: '1px solid #E1DFDD', flexShrink: 0, zIndex: 100 }}>
        ©2026 <a href="https://ajalabs.ai" target="_blank" rel="noopener noreferrer" style={{ color: '#323130', textDecoration: 'none', fontWeight: 'bold' }}>ajaLabs.ai</a> All rights reserved - <a href="#" style={{ color: '#0078D4', textDecoration: 'none' }}>Data Privacy</a>
      </div>

    </div>
  );
}