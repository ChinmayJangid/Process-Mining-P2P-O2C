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
  blue700:'#0078D4', teal:'#038387', red:'#D13438', purple:'#5C2D91',
  slate:'#605E5C', bg:'#F0F2F5', card:'#FFFFFF', border:'#E1DFDD',
  orange:'#CA5010', green:'#107C10', selected:'#EFF6FF', selectedBorder:'#0078D4',
  headerBg:'#1B2A4A', mapNodeBg: '#A5C6D9', mapNodeBorder: '#648596', mapEdge: '#8B9CB3',
  jkBlue: '#0057B7' 
};
const ACCENT=['#0078D4','#038387','#CA5010','#D13438','#5C2D91','#E3008C',
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
      background:'rgba(27,42,74,0.92)',backdropFilter:'blur(8px)',
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
            background:'linear-gradient(90deg,#0078D4,#00B7C3)',
            width:`${progress}%`,boxShadow:'0 0 12px rgba(0,120,212,.6)'}}/>
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
      {name&&<div style={{fontWeight:600,marginBottom:4,color:'#0078D4',wordBreak:'break-word'}}>{name}</div>}
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

/* ─── PROCESS NODE ────────────────────────────────────────────── */
const ProcessNode=React.memo(({data})=>{
  const freq=data?.frequency||0;
  const isHappyPath = data?.is_main;
  
  return(
    <div style={{
      background: isHappyPath ? '#7ebe42' : '#999999', 
      border: '2px solid #064f86', 
      borderRadius: 8,
      minWidth: 550, 
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
      <div style={{
        fontSize: 50, 
        fontWeight: 700, 
        color: '#102A43', 
        lineHeight: 1.3,
        marginBottom: 8,
        width: '200%',
        wordBreak: 'break-word'
      }}>
        {data?.label||''}
      </div>
      <div style={{alignSelf:'center'}}>
        <span style={{
          fontSize: 35, 
          fontWeight: 600,
          color: '#005A9E', 
          backgroundColor: 'rgba(255,255,255,0.7)',
          borderRadius: 12,
          padding: '3px 12px',
          border: '1px solid rgba(0,0,0,0.08)',
          whiteSpace: 'nowrap'
        }}>
          {freq>0?Number(freq).toLocaleString():'0'} cases
        </span>
      </div>

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

/* ─── FREQ EDGE (Visible Labels) ──────────────────────────────── */
const cubicBezierPoint = (p0, p1, p2, p3, t) => {
  const mt = 1 - t;
  return mt*mt*mt*p0 + 3*mt*mt*t*p1 + 3*mt*t*t*p2 + t*t*t*p3;
};

const FreqEdge=React.memo(({id,sourceX,sourceY,targetX,targetY,sourcePosition,targetPosition,data,markerEnd,style})=>{
  const curvature  = data?.curvature  ?? 0.5;
  const sweepSide  = data?.sweepSide;
  const sweepDist  = data?.sweepDist  ?? 120;
  const freq       = data?.frequency  || 0;
  const max        = data?.maxFreq    || 1;
  const width      = 1 + (freq / max) * 4;
  const arcColor   = '#605E5C';

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
      <BaseEdge id={id} path={edgePath} markerEnd={{...markerEnd, color: arcColor}}
        style={{...style, stroke: arcColor, strokeWidth: width, opacity:.85}}/>
      {freq>0&&(
        <EdgeLabelRenderer>
          <div style={{position:'absolute',
            transform:`translate(-50%,-50%) translate(${labelX}px,${labelY}px)`,
            pointerEvents:'all',zIndex:100,
            display:'flex',flexDirection:'column',alignItems:'center',gap:2}}>
            <div style={{fontSize:25,fontWeight:700,color:'#323130',background:'rgba(255,255,255,0.95)',
              border:'1px solid #E1DFDD', padding:'1px 6px',borderRadius:4,
              boxShadow:'0 2px 4px rgba(0,0,0,0.12)'}}>
              {Number(freq).toLocaleString()}
            </div>
            {data?.avg_days!=null&&(
              <div style={{fontSize:25,color:'#605E5C',background:'rgba(255,255,255,.9)',
                padding:'0 4px',borderRadius:2}}>
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

/* ─── HAPPY PATH ORDER ─────────────────────────────────────────── */
const HAPPY_PATH = [
  "PR Creation", "PR Release Date", "PO Creation",
  "PO Date", "GR Posting", "Invoice Posting"
];
const HAPPY_IDX = Object.fromEntries(HAPPY_PATH.map((n,i)=>[n,i]));

const SIDE_ABOVE_LR = new Set(["PR Reversal","PR Reversal (Post-PO)","PO Reversal","GR Reversal","Invoice Reversal Date"]);
const SIDE_BELOW_LR = new Set(["PO Reversal (Post-GR)","GR Reversal (Post-Inv)"]);
const SIDE_LEFT_TB = new Set(["PR Reversal","PO Reversal","GR Reversal","Invoice Reversal Date"]);
const SIDE_RIGHT_TB = new Set(["PR Reversal (Post-PO)","PO Reversal (Post-GR)","GR Reversal (Post-Inv)"]);

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
const VALID_KEYS=new Set(['company','bsart','matkl','vendor','plant','purch_group',
  'case_id','month','activity','year','quarter','lifnr', 'lead_time', 'ernam', 'status', 'sod']);
const qs=(params)=>{
  const p=Object.entries(params).filter(([k,v])=>VALID_KEYS.has(k)&&v&&v!=='ALL');
  return p.length?'?'+p.map(([k,v])=>`${k}=${encodeURIComponent(v)}`).join('&'):'';
};

const CROSS_TO_PARAM={
  company:'company', bsart:'bsart', matkl:'matkl', vendor:'vendor', plant:'plant',
  activity:'activity', month:'month', year:'year', quarter:'quarter',
  case_id:'case_id', lifnr:'lifnr', purch_group:'purch_group', lead_time:'lead_time', ernam:'ernam',
  status:'status', sod:'sod'
};

const Empty=()=>(
  <div style={{height:90,display:'flex',alignItems:'center',justifyContent:'center',
    color:C.slate,fontSize:12}}>No data available</div>
);

/* ─── KPI CARD ─────────────────────────────────────────────────── */
const KpiCard=React.memo(({label,value,color,highlighted,onClick, tooltip})=>{
  const [hover,setHover]=useState(false);
  const bColor = hover ? 'rgba(106,51,130,0.5)' : (highlighted ? C.selectedBorder : 'transparent');
  const bWidth = '1.5px';

  return(
    <div onClick={onClick} onMouseEnter={()=>setHover(true)} onMouseLeave={()=>setHover(false)} style={{
      background:highlighted?C.selected:C.card,borderRadius:6,padding:'10px 14px',
      borderTop: `${bWidth} solid ${bColor}`, borderRight: `${bWidth} solid ${bColor}`,
      borderBottom: `${bWidth} solid ${bColor}`, borderLeft:`4px solid #6a3382`,
      boxShadow: hover ? '0 6px 16px rgba(106,51,130,.15)' : '0 2px 6px rgba(0,0,0,.05)',
      transition:'all .2s', cursor:onClick?'pointer':'default',minWidth:0, position:'relative',
      display:'flex',flexDirection:'column',alignItems:'center', textAlign:'center',
      transform: hover ? 'translateY(-3px)' : 'none', boxSizing: 'border-box',
      zIndex: hover ? 50 : 1 
    }}>
      <div style={{fontSize:10,fontWeight:600,color:"#6a3382",textTransform:'uppercase',
        letterSpacing:.5,marginBottom:2}}>{label}</div>
      <div style={{fontSize:20,fontWeight:600,color:'#000000',lineHeight:1}}> 
        {value!=null?Number(value).toLocaleString():'—'}
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
});

const ConfKpiCard=React.memo(({label,value,color,sub,tooltip,onClick,highlighted})=>{
  const [hover,setHover]=useState(false);
  const bColor = hover ? 'rgba(106,51,130,0.5)' : (highlighted ? C.selectedBorder : 'transparent');
  const bWidth = '1.5px';

  return(
    <div onClick={onClick} onMouseEnter={()=>setHover(true)} onMouseLeave={()=>setHover(false)} style={{
      background:highlighted?C.selected:C.card,borderRadius:6,padding:'10px 14px',
      borderTop: `${bWidth} solid ${bColor}`, borderRight: `${bWidth} solid ${bColor}`,
      borderBottom: `${bWidth} solid ${bColor}`, borderLeft:`4px solid #6a3382`,
      boxShadow: hover ? '0 6px 16px rgba(106,51,130,.15)' : '0 2px 6px rgba(0,0,0,.05)',
      transition:'all .2s', cursor:onClick?'pointer':'default',minWidth:0, position:'relative',
      display:'flex',flexDirection:'column',alignItems:'center', textAlign:'center',
      transform: hover ? 'translateY(-3px)' : 'none', boxSizing: 'border-box',
      zIndex: hover ? 50 : 1 
    }}>
      <div style={{fontSize:10,fontWeight:600,color: "#6a3382",textTransform:'uppercase',
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
});

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

/* ─── STANDARD SELECT ──────────────────────────────────────────── */
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

/* ─── CHART CARD ───────────────────────────────────────────────── */
const ChartCard=React.memo(({title,subtitle,children,highlighted,onClear,style={}, loading=false})=>(
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

/* ══════════════════════════════════════════
   CHARTS
══════════════════════════════════════════ */

const ActivityChart=React.memo(({data,crossFilter,onSelect})=>{
  if(!Array.isArray(data)||!data.length) return <EmptyState condition={true} message="Data Not Uploaded / Available" />;
  const af=crossFilter?.type==='activity'?crossFilter.value:null;
  const rows=data.slice(0,7); 
  return(
    <div style={{display:'flex',flexDirection:'column',gap:6}}>
      <div style={{display:'flex',gap:14}}>
        {[['#0078D4','Events'],['#01a32a','Unique Cases']].map(([c,l])=>(
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
                <Cell key={i} cursor="pointer" fill={af===e?.activity?'#CA5010':'#0078D4'} opacity={af&&af!==e?.activity?0.35:1}/>
              ))}
            </Bar>
            
            <Bar dataKey="unique_cases" radius={[0,3,3,0]} barSize={20}
              onClick={e=>e?.activity&&onSelect('activity',e.activity===af?null:e.activity)}>
              {rows.map((e,i)=>(
                <Cell key={i} cursor="pointer" fill={af===e?.activity?'#999999':'#268703'} opacity={af&&af!==e?.activity?0.3:0.9}/>
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
});

const MonthlyChart=React.memo(({data,crossFilter,onSelect})=>{
  if(!Array.isArray(data)||!data.length) return <EmptyState condition={true} message="Data Not Uploaded / Available" />;
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
            type="monotone" dataKey="count" stroke="#0078D4" strokeWidth={2.5} cursor="pointer"
            dot={{r:3, fill:'#0078D4'}} 
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

const CompanyDonutChart=React.memo(({data,crossFilter,onSelect})=>{
  if(!Array.isArray(data)||!data.length) return <EmptyState condition={true} message="Data Not Uploaded / Available" />;
  const af=crossFilter?.type==='company'?crossFilter.value:null;
  const total=data.reduce((s,d)=>s+(d.count||0),0);

  const chartData = data.map((d, i) => ({
    ...d,
    fill: ACCENT[i % ACCENT.length]
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
  return(
    <div style={{width:'100%',height:260, position:'relative'}}>
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Pie data={chartData} dataKey="count" nameKey="company"
            cx="40%" cy="50%" innerRadius={60} outerRadius={95}
            paddingAngle={2} labelLine={false} label={renderLabel}
            onClick={e=>e?.company&&onSelect('company',e.company===af?null:e.company)}>
            {chartData.map((entry,i)=>(
              <Cell key={i} fill={entry.fill} cursor="pointer"
                opacity={af&&af!==entry?.company?0.2:1}
                stroke={af===entry?.company?'#323130':'none'}
                strokeWidth={af===entry?.company?2:0}/>
            ))}
          </Pie>
          <text x="38%" y="46%" textAnchor="middle" dominantBaseline="middle" style={{fontSize:18,fontWeight:800,fill:'#323130'}}>
            {Number(af?(chartData.find(d=>d.company===af)?.count||0):total).toLocaleString()}
          </text>
          <text x="38%" y="56%" textAnchor="middle" dominantBaseline="middle" style={{fontSize:10,fill:'#8A8886',fontWeight:600}}>
            {af||'Total'}
          </text>
          <Tooltip formatter={v=>[Number(v).toLocaleString(),'Cases']} contentStyle={{background:'#fff',border:`1px solid ${C.border}`,borderRadius:6,fontSize:12}}/>
          <Legend 
            layout="vertical" 
            verticalAlign="middle" 
            align="right" 
            iconType="circle" 
            wrapperStyle={{fontSize: '11px', cursor: 'pointer', right: 10}} 
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

const StatusDonutChart=React.memo(({data,crossFilter,onSelect})=>{
  if(!Array.isArray(data)||!data.length) return <EmptyState condition={true} message="Data Not Uploaded / Available" />;
  const af=crossFilter?.type==='status'?crossFilter.value:null;
  const total=data.reduce((s,d)=>s+(d.count||0),0);

  const chartData = data.map((d) => ({
    ...d,
    fill: d.status === 'Happy Path' ? '#107C10' : '#D13438'
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
  return(
    <div style={{width:'100%',height:260, position:'relative'}}>
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Pie data={chartData} dataKey="count" nameKey="status"
            cx="40%" cy="50%" innerRadius={60} outerRadius={95}
            paddingAngle={2} labelLine={false} label={renderLabel}
            onClick={e=>e?.status&&onSelect('status',e.status===af?null:e.status)}>
            {chartData.map((entry,i)=>(
              <Cell key={i} fill={entry.fill} cursor="pointer"
                opacity={af&&af!==entry?.status?0.2:1}
                stroke={af===entry?.status?'#323130':'none'}
                strokeWidth={af===entry?.status?2:0}/>
            ))}
          </Pie>
          <text x="36%" y="46%" textAnchor="middle" dominantBaseline="middle" style={{fontSize:18,fontWeight:800,fill:'#323130'}}>
            {Number(af?(chartData.find(d=>d.status===af)?.count||0):total).toLocaleString()}
          </text>
          <text x="36%" y="56%" textAnchor="middle" dominantBaseline="middle" style={{fontSize:10,fill:'#8A8886',fontWeight:600}}>
            {af||'Total Cases'}
          </text>
          <Tooltip formatter={v=>[Number(v).toLocaleString(),'Cases']} contentStyle={{background:'#fff',border:`1px solid ${C.border}`,borderRadius:6,fontSize:12}}/>
          <Legend 
            layout="vertical" 
            verticalAlign="middle" 
            align="right" 
            iconType="circle" 
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

const ScrollableHBarChart=React.memo(({data,dataKey,labelKey,crossFilter,crossKey,onSelect,color})=>{
  if(!Array.isArray(data)||!data.length) return <EmptyState condition={true} message="Data Not Uploaded / Available" />;
  const af=crossFilter?.type===crossKey?crossFilter.value:null;
  const rowH=30;
  const chartH=Math.max(220, data.length*rowH);
  return(
    <div style={{width:'100%',height:220, overflowY:'auto', paddingRight:8}}>
      <div style={{height:chartH}}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} layout="vertical" margin={{left:10,right:20,top:4,bottom:4}}>
            <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" horizontal={false}/>
            <XAxis type="number" hide/>
            <YAxis type="category" dataKey={labelKey} tick={{fontSize:10,fill:'#605E5C'}} width={120} interval={0}/>
            <Tooltip cursor={{fill:'rgba(0,0,0,.04)'}} content={<CustomTooltip nameKey={labelKey} labelOverride="cases"/>}/>
            <Bar dataKey={dataKey} radius={[0,3,3,0]} barSize={20}
              onClick={e=>e&&e[labelKey]&&onSelect(crossKey,e[labelKey]===af?null:e[labelKey])}>
              {data.map((entry,i)=>(
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

const ScrollableVBarChart=React.memo(({data,crossFilter,onSelect})=>{
  if(!Array.isArray(data)||!data.length) return <EmptyState condition={true} message="Data Not Uploaded / Available" />;
  const af=crossFilter?.type==='bsart'?crossFilter.value:null;
  const colW=50;
  const chartW=Math.max('100%', data.length*colW);
  return(
    <div style={{width:'100%',height:220, overflowX:'auto', overflowY:'hidden'}}>
      <div style={{width:chartW, height:'100%'}}>
        <ResponsiveContainer width="100%" height="120%">
          <BarChart data={data} margin={{left:8,right:16,top:10,bottom:40}}>
            <CartesianGrid strokeDasharray="3 3" stroke="#EDEBE9" vertical={false}/>
            <XAxis dataKey="bsart" tick={{fontSize:11,fill:'#605E5C'}} angle={-40} textAnchor="end" interval={0}/>
            <YAxis tick={{fontSize:10,fill:'#605E5C'}} width={40}/>
            <Tooltip cursor={{fill:'rgba(0,0,0,.04)'}} content={<CustomTooltip nameKey="bsart" labelOverride="cases"/>}/>
            <Bar dataKey="count" radius={[4,4,0,0]}
              onClick={e=>e&&e.bsart&&onSelect('bsart',e.bsart===af?null:e.bsart)}>
              {data.map((entry,i)=>(
                <Cell key={i} cursor="pointer" fill={ACCENT[i%ACCENT.length]} opacity={af&&af!==entry?.bsart?0.25:1}/>
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
});

const LeadTimeChart=React.memo(({data, crossFilter, onSelect})=>{
  if(!Array.isArray(data)||!data.length) return <EmptyState condition={true} message="Data Not Uploaded / Available" />;
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

const ErnamChart=React.memo(({data, crossFilter, onSelect})=>{
    if(!Array.isArray(data)||!data.length) return <EmptyState condition={true} message="Data Not Uploaded / Available" />;
    
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
                    <Cell key={i} cursor="pointer" fill="#2254b1" opacity={af&&af!==entry?.ernam?0.25:1}/>
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
    <div style={{ width: '100%', height: 260, overflowX: 'auto', overflowY: 'hidden', minWidth: 0, maxWidth: '100%' }}>
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
    <div style={{borderBottom:'1px solid #E2E8F0'}}>
      <button
        onClick={() => setOpen(o => !o)}
        style={{
          width:'100%', display:'flex', justifyContent:'space-between', alignItems:'center',
          padding:'14px 0', background:'none', border:'none', cursor:'pointer', textAlign:'left', gap:12,
        }}>
        <span style={{fontSize:13.5, fontWeight:600, color:'#1e293b', lineHeight:1.4}}>{q}</span>
        <span style={{
          fontSize:18, color:accent, flexShrink:0, fontWeight:700,
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
    a: 'Process Mining analyzes real event log data from ERP systems (SAP, Oracle) to reconstruct and visualize how the P2P process actually runs — not just how it was designed. In P2P it creates a "digital twin" of procurement activities, revealing:',
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
    q: 'What are the most common pain points Process Mining uncovers in P2P?',
    a: 'Typical discoveries include:',
    bullets: [
      'High number of process variants ("spaghetti processes")',
      'Maverick buying — purchases bypassing approved channels',
      'Long approval or PO cycle times',
      'Low PO compliance or three-way matching rates',
      'Missed cash and early-payment discounts',
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

const P2PIntroScreen = ({ onGoTableBuild, onGoCsvUpload, currentUser }) => {
  const [introStep, setIntroStep] = useState('overview'); // 'overview' | 'choose'

  const steps = [
    'Purchase Requisition (PR) is raised by a department',
    'PR is approved and converted to a Purchase Order (PO)',
    'PO is sent to vendor — Goods Receipt (GR) is posted on delivery',
    'Vendor submits invoice — Invoice Receipt (IR) is recorded in SAP',
    'Three-way match: PO vs GR vs IR is verified',
    'Finance clears the invoice and payment is made to the vendor',
  ];
  const kpis = [
    'PR-to-PO conversion time','PO-to-GR lead time','GR-to-IR matching rate',
    'Invoice processing cycle time','Three-way match exception rate','Vendor on-time delivery %',
  ];

  if (introStep === 'overview') return (
    <div style={{flex:1,display:'flex',flexDirection:'column',alignItems:'center',padding:'36px 24px 48px',overflowY:'auto'}}>
      <div style={{maxWidth:880,width:'100%',display:'flex',flexDirection:'column',gap:24}}>

        {/* Header */}
        <div style={{borderBottom:'2px solid #E2E8F0',paddingBottom:20}}>
          <div style={{fontSize:11,fontWeight:700,color:'#0078D4',textTransform:'uppercase',letterSpacing:1,marginBottom:6}}>Process Overview</div>
          <h1 style={{margin:'0 0 8px',fontSize:24,fontWeight:700,color:'#1e293b'}}>Procure-to-Pay (P2P)</h1>
          <p style={{margin:0,fontSize:13.5,color:'#475569',lineHeight:1.7,maxWidth:720}}>
            The Procure-to-Pay process spans the full purchasing lifecycle — from identifying a business need
            through raising a requisition, placing a purchase order with a vendor, receiving goods, and finally
            clearing the vendor invoice. Process mining on P2P data helps uncover delays, duplicate payments,
            three-way match failures, and segregation-of-duties violations.
          </p>
        </div>

        {/* Process Steps + KPIs */}
        <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:20}}>
          <div style={{background:'#fff',border:'1px solid #E2E8F0',borderRadius:10,padding:'20px 22px',boxShadow:'0 2px 6px rgba(0,0,0,0.04)'}}>
            <div style={{fontSize:11,fontWeight:700,color:'#64748b',textTransform:'uppercase',letterSpacing:0.8,marginBottom:14}}>Process Steps</div>
            <ol style={{margin:0,padding:'0 0 0 18px',display:'flex',flexDirection:'column',gap:10}}>
              {steps.map((s,i)=>(<li key={i} style={{fontSize:13,color:'#334155',lineHeight:1.5}}><span style={{fontWeight:600,color:'#0078D4'}}>Step {i+1}.</span>{' '}{s}</li>))}
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
            {P2P_FAQS.map((faq, i) => <FaqItem key={i} q={faq.q} a={faq.a} bullets={faq.bullets} accentColor="#0078D4" />)}
          </div>
        </div>

        {/* Continue button — bottom right */}
        <div style={{display:'flex',justifyContent:'flex-end',paddingTop:4}}>
          <button
            onClick={() => setIntroStep('choose')}
            style={{
              background:'#0078D4', color:'#fff', border:'none',
              padding:'12px 32px', borderRadius:8, fontSize:14, fontWeight:700,
              cursor:'pointer', display:'flex', alignItems:'center', gap:8,
              boxShadow:'0 4px 12px rgba(0,120,212,0.25)', transition:'all 0.2s',
            }}
            onMouseOver={e=>{e.currentTarget.style.background='#005A9E';e.currentTarget.style.boxShadow='0 6px 16px rgba(0,120,212,0.35)';}}
            onMouseOut={e=>{e.currentTarget.style.background='#0078D4';e.currentTarget.style.boxShadow='0 4px 12px rgba(0,120,212,0.25)';}}>
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
          {/* Build Event Log card */}
          <div style={{background:'#fff',border:'2px solid #E2E8F0',borderRadius:12,padding:'28px 24px',
            display:'flex',flexDirection:'column',alignItems:'center',gap:14,textAlign:'center',
            boxShadow:'0 2px 8px rgba(0,0,0,0.05)',transition:'all 0.2s',cursor:'pointer'}}
            onClick={onGoTableBuild}
            onMouseEnter={e=>{e.currentTarget.style.borderColor='#006B3C';e.currentTarget.style.boxShadow='0 4px 16px rgba(0,107,60,0.15)';}}
            onMouseLeave={e=>{e.currentTarget.style.borderColor='#E2E8F0';e.currentTarget.style.boxShadow='0 2px 8px rgba(0,0,0,0.05)';}}>
            <div style={{width:60,height:60,borderRadius:14,background:'#EDFAF4',
              display:'flex',alignItems:'center',justifyContent:'center',fontSize:28}}>
              🔨
            </div>
            <div>
              <div style={{fontSize:16,fontWeight:700,color:'#1e293b',marginBottom:8}}>Build Event Log</div>
              <div style={{fontSize:13,color:'#64748b',lineHeight:1.65}}>
                Upload raw SAP tables (EKKO, EKPO, EBAN, EKBE, LFA1) and let the system
                automatically build the process event log.
              </div>
            </div>
            <button
              onClick={e=>{e.stopPropagation();onGoTableBuild();}}
              style={{background:'#006B3C',color:'#fff',border:'none',padding:'11px 28px',
                borderRadius:7,fontSize:13,fontWeight:700,cursor:'pointer',width:'100%',marginTop:'auto'}}
              onMouseOver={e=>e.currentTarget.style.background='#004d2c'}
              onMouseOut={e=>e.currentTarget.style.background='#006B3C'}>
              Build Event Log
            </button>
          </div>

          {/* Upload Pre-built CSV card */}
          <div style={{background:'#fff',border:'2px solid #E2E8F0',borderRadius:12,padding:'28px 24px',
            display:'flex',flexDirection:'column',alignItems:'center',gap:14,textAlign:'center',
            boxShadow:'0 2px 8px rgba(0,0,0,0.05)',transition:'all 0.2s',cursor:'pointer'}}
            onClick={onGoCsvUpload}
            onMouseEnter={e=>{e.currentTarget.style.borderColor='#0078D4';e.currentTarget.style.boxShadow='0 4px 16px rgba(0,120,212,0.15)';}}
            onMouseLeave={e=>{e.currentTarget.style.borderColor='#E2E8F0';e.currentTarget.style.boxShadow='0 2px 8px rgba(0,0,0,0.05)';}}>
            <div style={{width:60,height:60,borderRadius:14,background:'#EFF6FF',
              display:'flex',alignItems:'center',justifyContent:'center',fontSize:28}}>
              📂
            </div>
            <div>
              <div style={{fontSize:16,fontWeight:700,color:'#1e293b',marginBottom:8}}>Upload Pre-built CSV</div>
              <div style={{fontSize:13,color:'#64748b',lineHeight:1.65}}>
                Already have a formatted event log? Upload your pre-built CSV file directly to launch the dashboard.
              </div>
            </div>
            <button
              onClick={e=>{e.stopPropagation();onGoCsvUpload();}}
              style={{background:'#0078D4',color:'#fff',border:'none',padding:'11px 28px',
                borderRadius:7,fontSize:13,fontWeight:700,cursor:'pointer',width:'100%',marginTop:'auto'}}
              onMouseOver={e=>e.currentTarget.style.background='#005A9E'}
              onMouseOut={e=>e.currentTarget.style.background='#0078D4'}>
              Upload Pre-built CSV
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

/* ── Table Upload Screen (Build Event Log path) ────────────────────────── */
const TableUploadScreen = ({ onBuilt, onBack, onLoadingChange, currentUser, myFiles, fetchingFiles, handleLoadOldFile }) => {
  const tables = [
    { name:'EKKO', desc:'Purchasing Document Header', isMandatory: true, required:[
      {col:'EBELN',  note:'PO number — join key'},
      {col:'AEDAT',  note:'PO creation date → PO Creation'},
      {col:'BEDAT',  note:'PO document date → PO Date'},
      {col:'BSART',  note:'Document type — filter & chart'},
      {col:'LIFNR',  note:'Vendor ID — filter & chart'},
      {col:'BUKRS',  note:'Company code — filter & chart'},
      {col:'EKGRP',  note:'Purchasing group — filter & chart'},
      {col:'ERNAM',  note:'PO creator — SOD checks & chart'},
      {col:'LOEKZ',  note:'Deletion flag — PO Reversal rule'},
    ]},
    { name:'EKPO', desc:'Purchasing Document Item', isMandatory: true, required:[
      {col:'EBELN',  note:'PO number — join key'},
      {col:'EBELP',  note:'PO item — part of UniqueID_PO'},
      {col:'MATNR',  note:'Material number'},
      {col:'WERKS',  note:'Plant — filter & chart'},
      {col:'MATKL',  note:'Material group — filter & chart'},
      {col:'BANFN',  note:'PR number — part of UniqueID_PR'},
      {col:'BNFPO',  note:'PR item — part of UniqueID_PR'},
      {col:'LOEKZ',  note:'Deletion flag — PO Reversal Date'},
      {col:'AEDAT',  note:'Item change date'},
    ]},
    { name:'EBAN', desc:'Purchase Requisition', required:[
      {col:'BANFN',  note:'PR number — join key'},
      {col:'BNFPO',  note:'PR item — join key'},
      {col:'BADAT',  note:'PR requirement date → PR Creation'},
      {col:'FRGDT',  note:'PR release date → PR Release Date'},
      {col:'ERNAM',  note:'PR creator — SOD checks & chart'},
      {col:'ERDAT',  note:'PR creation date — PR Reversal rule'},
      {col:'LOEKZ',  note:'Deletion flag — PR Reversal rule'},
    ]},
    { name:'EKBE', desc:'PO History / GR & Invoice Events', required:[
      {col:'EBELN',  note:'PO number — join key'},
      {col:'EBELP',  note:'PO item — join key'},
      {col:'VGABE',  note:'Movement type: 1=GR, 2=Invoice'},
      {col:'BUDAT',  note:'Posting date → GR / Invoice dates'},
      {col:'SHKZG',  note:'Debit/Credit: S=normal, H=reversal'},
      {col:'ERNAM',  note:'Posting user — GR/Invoice Creation User'},
      {col:'BELNR',  note:'Accounting document number'},
      {col:'GJAHR',  note:'Fiscal year'},
      {col:'MENGE',  note:'Quantity'},
      {col:'DMBTR',  note:'Amount in local currency'},
    ]},
    { name:'LFA1', desc:'Vendor Master — General', required:[
      {col:'LIFNR',  note:'Vendor ID — join key'},
      {col:'NAME1',  note:'Vendor name — vendor filter & chart'},
    ]},
  ];
  const [tableStatus, setTableStatus] = useState(Object.fromEntries(tables.map(t=>[t.name,'idle'])));
  const [tableMsg,    setTableMsg]    = useState(Object.fromEntries(tables.map(t=>[t.name,''])));
  const [building,    setBuilding]    = useState(false);
  const [buildMsg,    setBuildMsg]    = useState('');
  const [colMapping,  setColMapping]  = useState(null); // {tableName, file, tableDef, uploadedCols:[], mapping:{}}
  const [tableCols,   setTableCols]   = useState({});
  const [selectedFiles, setSelectedFiles] = useState({});
  const [appliedMappings, setAppliedMappings] = useState({});

  const fileRefs = useRef(Object.fromEntries(tables.map(t=>[t.name,React.createRef()])));

  const allDone      = tables.filter(t=>t.isMandatory).every(t=>tableStatus[t.name]==='done');
  const anyUploading = tables.some(t=>tableStatus[t.name]==='uploading')||building;

  // ── Restore already-uploaded tables from server on mount ──────────────────
  React.useEffect(()=>{
    fetch(`${API}/p2p/transform/status?username=${encodeURIComponent(currentUser||'Unknown')}`)
      .then(r=>r.ok?r.json():null)
      .then(d=>{
        if(!d||!d.loaded) return;
        d.loaded.forEach(tName=>{
          setTableStatus(p=>({...p,[tName]:'done'}));
          setTableMsg(p=>({...p,[tName]:'Already on server'}));
        });
      }).catch(()=>{});
  // eslint-disable-next-line react-hooks/exhaustive-deps
  },[]);

  const uploadTable = async(tableName, file)=>{
    if(!file) return;
    if(!file.name.toLowerCase().endsWith('.csv')){
      setTableStatus(p=>({...p,[tableName]:'error'}));
      setTableMsg(p=>({...p,[tableName]:'Only .csv accepted.'}));
      return;
    }
    setSelectedFiles(p=>({...p, [tableName]: file}));
    performUpload(tableName, file, {});
  };

  const handleMapColumns = async (tableName) => {
    const file = selectedFiles[tableName];
    if (!file) return;
    const formPreview=new FormData();
    formPreview.append('file',file);
    try{
      const rPrev=await fetch(`${API}/p2p/transform/preview_columns`,{method:'POST',body:formPreview});
      const dPrev=await rPrev.json();
      if(!rPrev.ok) throw new Error(dPrev.detail||`Failed to read CSV columns`);
      const tDef=tables.find(t=>t.name===tableName);
      setColMapping({ tableName, file, tableDef: tDef, uploadedCols: dPrev.columns, mapping: {} });
    }catch(e){
      setTableStatus(p=>({...p,[tableName]:'error'}));
      setTableMsg(p=>({...p,[tableName]:e.message}));
    }
  };

  const performUpload = async(tableName, file, mapping)=>{
    setTableStatus(p=>({...p,[tableName]:'uploading'}));
    setTableMsg(p=>({...p,[tableName]:''}));
    const form=new FormData();
    form.append('file',file); form.append('table_name',tableName); form.append('username',currentUser||'Unknown');
    form.append('column_mapping', JSON.stringify(mapping));
    try{
      const r=await fetch(`${API}/p2p/transform/upload_table`,{method:'POST',body:form});
      const d=await r.json();
      if(!r.ok) throw new Error(d.detail||`HTTP ${r.status}`);
      setTableStatus(p=>({...p,[tableName]:'done'}));
      setTableMsg(p=>({...p,[tableName]:`${Number(d.rows).toLocaleString()} rows`}));
      if(d.columns){
        setTableCols(p=>({...p,[tableName]:d.columns}));
      }
      setColMapping(null);
    }catch(e){
      setTableStatus(p=>({...p,[tableName]:'error'}));
      setTableMsg(p=>({...p,[tableName]:e.message}));
    }
  };

  const handleBuild=async()=>{
    setBuilding(true); setBuildMsg('');
    onLoadingChange&&onLoadingChange(true,20,'Processing Data...');
    let prog=20;
    const ticker=setInterval(()=>{prog=Math.min(prog+Math.random()*14,88);onLoadingChange&&onLoadingChange(true,prog,'Analysing Data...');},400);
    try{
      const r=await fetch(`${API}/p2p/transform/build?username=${encodeURIComponent(currentUser||'Unknown')}`,{method:'POST'});
      const d=await r.json();
      clearInterval(ticker);
      if(!r.ok) throw new Error(d.detail||`HTTP ${r.status}`);
      onLoadingChange&&onLoadingChange(true,100,'Dashboard Created');
      setTimeout(()=>{setBuilding(false);setBuildMsg(`✓ Success: ${Number(d.rows).toLocaleString()} rows processed`);onBuilt&&onBuilt();},800);
    }catch(e){
      clearInterval(ticker);
      onLoadingChange&&onLoadingChange(false,0,'');
      setBuilding(false);
      if (e.message.includes('Column mapping is incorrect')) {
        onBuilt();
      } else {
        setBuildMsg(`Error: ${e.message}`);
      }
    }
  };

  const si=(s)=>{
    if(s==='done')     return{icon:'✓',color:'#107C10',bg:'#F0FAF0',border:'#107C10'};
    if(s==='error')    return{icon:'✕',color:'#D13438',bg:'#FDE7E9',border:'#D13438'};
    if(s==='uploading')return{icon:'…',color:'#0078D4',bg:'#EFF6FF',border:'#0078D4'};
    return                   {icon:'↑',color:'#0078D4',bg:'#fff',   border:'#0078D4'};
  };

  const tableBuilds=(myFiles||[]).filter(f=>f.source==='table_build');

  return(
    <div style={{flex:1,display:'flex',flexDirection:'column',alignItems:'center',padding:'28px 24px 48px',overflowY:'auto'}}>



      {/* ══ Column Mapping Modal ══════════════════════════════════════════════ */}
      {colMapping&&(
        <div style={{position:'fixed',inset:0,zIndex:9999,background:'rgba(0,0,0,0.5)',
          display:'flex',alignItems:'center',justifyContent:'center',padding:16}}
          onClick={()=>setColMapping(null)}>
          <div style={{background:'#fff',borderRadius:12,width:'100%',maxWidth:700,
            maxHeight:'88vh',display:'flex',flexDirection:'column',
            boxShadow:'0 24px 64px rgba(0,0,0,0.35)'}}
            onClick={e=>e.stopPropagation()}>
            <div style={{padding:'20px 24px 16px',borderBottom:'1px solid #E2E8F0',flexShrink:0}}>
              <div style={{fontSize:10,fontWeight:700,color:'#DC2626',textTransform:'uppercase',letterSpacing:1,marginBottom:4}}>Column Mapping</div>
              <div style={{fontSize:17,fontWeight:700,color:'#1e293b'}}>Map Columns for {colMapping.tableDef.name}</div>
              <div style={{fontSize:12,color:'#64748b',marginTop:3}}> Select which columns from your file correspond to the required fields.</div>
            </div>
            
            <div style={{overflowY:'auto',flex:1,padding:'0 0 8px'}}>
              <table style={{width:'100%',borderCollapse:'collapse',fontSize:12}}>
                <thead style={{position:'sticky',top:0,zIndex:2}}>
                  <tr style={{background:'#F8FAFC'}}>
                    <th style={{padding:'10px 16px',textAlign:'left',fontWeight:700,color:'#64748b',fontSize:10,textTransform:'uppercase',letterSpacing:0.8,borderBottom:'2px solid #E2E8F0',width:'35%'}}>Required Column</th>
                    <th style={{padding:'10px 12px',textAlign:'left',fontWeight:700,color:'#64748b',fontSize:10,textTransform:'uppercase',letterSpacing:0.8,borderBottom:'2px solid #E2E8F0',width:'35%'}}>Map to File Column</th>
                    <th style={{padding:'10px 12px',textAlign:'left',fontWeight:700,color:'#64748b',fontSize:10,textTransform:'uppercase',letterSpacing:0.8,borderBottom:'2px solid #E2E8F0'}}>Purpose</th>
                  </tr>
                </thead>
                <tbody>
                  {colMapping.tableDef.required.map((r,i)=>{
                    const reqCol = r.col;
                    const autoMatch = colMapping.uploadedCols.find(c=>c.toUpperCase()===reqCol.toUpperCase());
                    const selected = colMapping.mapping[reqCol] !== undefined ? colMapping.mapping[reqCol] : (autoMatch || '');
                    return(
                      <tr key={reqCol} style={{borderBottom:'1px solid #F1F5F9',background:'#fff'}}>
                        <td style={{padding:'10px 16px',fontFamily:'monospace',fontWeight:700,color:'#334155',fontSize:13}}>{reqCol}</td>
                        <td style={{padding:'10px 12px'}}>
                          <select 
                            value={selected} 
                            onChange={e=>setColMapping(p=>({...p, mapping:{...p.mapping, [reqCol]: e.target.value}}))}
                            style={{width:'100%',padding:'6px 8px',borderRadius:4,border:'1px solid #CBD5E1',background:'#fff',fontSize:12,color:'#334155'}}
                          >
                            <option value="">-- Leave Blank / Unmapped --</option>
                            {colMapping.uploadedCols.map(c=>(
                              <option key={c} value={c}>{c}</option>
                            ))}
                          </select>
                        </td>
                        <td style={{padding:'10px 12px',color:'#64748b',fontSize:11,lineHeight:1.4}}>{r.note}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            <div style={{padding:'14px 20px',borderTop:'1px solid #E2E8F0',flexShrink:0,display:'flex',justifyContent:'flex-end',gap:12}}>
              <button onClick={()=>setColMapping(null)}
                style={{padding:'8px 16px',background:'#fff',color:'#64748b',border:'1px solid #CBD5E1',borderRadius:6,fontSize:13,fontWeight:700,cursor:'pointer'}}>
                Cancel
              </button>
              <button 
                onClick={async ()=>{
                  const finalMapping = {};
                  colMapping.tableDef.required.forEach(r => {
                      const autoMatch = colMapping.uploadedCols.find(c=>c.toUpperCase()===r.col.toUpperCase());
                      const sel = colMapping.mapping[r.col] !== undefined ? colMapping.mapping[r.col] : (autoMatch || '');
                      if (sel) {
                          finalMapping[sel] = r.col;
                      }
                  });
                  await fetch(`${API}/p2p/transform/clear_table?table_name=${colMapping.tableDef.name}&username=${encodeURIComponent(currentUser||'Unknown')}`, { method: 'DELETE' }).catch(console.error);
                  setAppliedMappings(p => ({...p, [colMapping.tableDef.name]: finalMapping}));
                  performUpload(colMapping.tableDef.name, colMapping.file, finalMapping);
                }}
                style={{padding:'8px 16px',background:'#0078D4',color:'#fff',border:'none',borderRadius:6,fontSize:13,fontWeight:700,cursor:'pointer'}}>
                Confirm Mapping & Upload
              </button>
            </div>
          </div>
        </div>
      )}

      <div style={{maxWidth:820,width:'100%',display:'flex',flexDirection:'column',gap:20}}>
        {/* Back + header */}
        <div style={{display:'flex',alignItems:'center',gap:12}}>
          <button onClick={onBack}
            style={{background:'none',border:'1px solid #E2E8F0',padding:'6px 14px',borderRadius:6,
              fontSize:12,cursor:'pointer',color:'#64748b',fontWeight:600}}
            onMouseOver={e=>e.currentTarget.style.background='#F8FAFC'}
            onMouseOut={e=>e.currentTarget.style.background='none'}>
            ← Back
          </button>
          <div>
            <div style={{fontSize:11,fontWeight:700,color:'#006B3C',textTransform:'uppercase',letterSpacing:0.8}}>Build Event Log</div>
            <div style={{fontSize:13,color:'#64748b'}}>Upload SAP tables below, then click Build. Only EKKO and EKPO are mandatory.</div>
          </div>
        </div>

        {/* Table upload panel */}
        <div style={{background:'#fff',border:'1px solid #E2E8F0',borderRadius:10,padding:'20px 22px',boxShadow:'0 2px 6px rgba(0,0,0,0.04)'}}>
          <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:14}}>
            <div style={{fontSize:11,fontWeight:700,color:'#64748b',textTransform:'uppercase',letterSpacing:0.8}}>SAP Tables</div>
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
                    onChange={e=>{
                      const f=e.target.files[0]; e.target.value='';
                      if(f) uploadTable(t.name,f);
                    }}/>
                  <button onClick={()=>{ if(!isUp&&ref.current){ref.current.value='';ref.current.click();} }}
                    disabled={isUp}
                    style={{display:'flex',alignItems:'center',justifyContent:'center',width:28,height:28,
                      borderRadius:6,border:`1.5px solid ${s.border}`,background:s.bg,color:s.color,
                      cursor:isUp?'not-allowed':'pointer',fontWeight:700,fontSize:13,flexShrink:0}}
                    onMouseOver={e=>{if(!isUp)e.currentTarget.style.background='#DBEAFE';}}
                    onMouseOut={e=>{e.currentTarget.style.background=s.bg;}}>
                    {isUp?<span style={{animation:'spin 1s linear infinite',display:'inline-block'}}>↻</span>:s.icon}
                  </button>
                  <div style={{minWidth:52,fontFamily:'monospace',fontWeight:700,fontSize:13,color:'#0078D4',
                    background:'#EFF6FF',padding:'3px 8px',borderRadius:4,textAlign:'center',flexShrink:0}}>{t.name}</div>
                  <div style={{fontSize:13,color:'#475569',flex:1}}>
                    {t.desc}
                    {appliedMappings[t.name] && Object.keys(appliedMappings[t.name]).length > 0 && (
                      <div style={{fontSize:11, color:'#006B3C', marginTop:4, display:'flex', flexWrap:'wrap', gap:6}}>
                        {Object.entries(appliedMappings[t.name]).map(([k,v]) => (
                          <span key={k} style={{background:'#E6F4EA', padding:'2px 6px', borderRadius:4}}><strong>{k}</strong> → {v}</span>
                        ))}
                      </div>
                    )}
                  </div>
                  {/* Server-returned message (error or success) + clear button */}
                  <div style={{display:'flex',alignItems:'center',gap:5,marginLeft:'auto',flexShrink:0}}>
                    {tableMsg[t.name]&&(
                      <div style={{fontSize:11,fontWeight:600,maxWidth:260,lineHeight:1.3,
                        color:tableStatus[t.name]==='error'?'#DC2626':'#15803D',
                        background:tableStatus[t.name]==='error'?'#FEF2F2':'transparent',
                        padding:tableStatus[t.name]==='error'?'3px 6px':'0',
                        borderRadius:4,border:tableStatus[t.name]==='error'?'1px solid #FECACA':'none'}}>
                        {tableMsg[t.name]}
                      </div>
                    )}
                    {(tableStatus[t.name]==='done' || tableStatus[t.name]==='error')&&(
                      <div style={{display:'flex',gap:4,alignItems:'center'}}>
                        {selectedFiles[t.name]&&(
                          <button
                            onClick={()=>handleMapColumns(t.name)}
                            title='Map columns'
                            style={{fontSize:10,fontWeight:700,padding:'2px 8px',borderRadius:4,
                              background:tableStatus[t.name]==='error'?'#FEF2F2':'#EFF6FF',
                              color:tableStatus[t.name]==='error'?'#DC2626':'#1D4ED8',
                              border:tableStatus[t.name]==='error'?'1px solid #FCA5A5':'1px solid #93C5FD',
                              cursor:'pointer'}}
                            onMouseOver={e=>e.currentTarget.style.background=tableStatus[t.name]==='error'?'#FECACA':'#DBEAFE'}
                            onMouseOut={e=>e.currentTarget.style.background=tableStatus[t.name]==='error'?'#FEF2F2':'#EFF6FF'}>
                            Map Columns
                          </button>
                        )}
                        <button
                          onClick={()=>{
                            fetch(`${API}/p2p/transform/clear_table?table_name=${t.name}&username=${encodeURIComponent(currentUser||'Unknown')}`, { method: 'DELETE' }).catch(console.error);
                            setTableStatus(p=>({...p,[t.name]:'idle'}));
                            setTableMsg(p=>({...p,[t.name]:''}));
                            setSelectedFiles(p=>{const copy={...p};delete copy[t.name];return copy;});
                            setAppliedMappings(p=>{const copy={...p};delete copy[t.name];return copy;});
                            if(fileRefs.current[t.name]?.current) fileRefs.current[t.name].current.value='';
                          }}
                          title='Clear to re-upload'
                          style={{display:'flex',alignItems:'center',justifyContent:'center',
                            width:18,height:18,borderRadius:'50%',border:'1.5px solid #FCA5A5',
                            background:'#FEE2E2',color:'#DC2626',cursor:'pointer',
                            fontWeight:800,fontSize:10,padding:0,lineHeight:1,flexShrink:0}}
                          onMouseOver={e=>e.currentTarget.style.background='#FECACA'}
                          onMouseOut={e=>e.currentTarget.style.background='#FEE2E2'}>
                          ✕
                        </button>
                      </div>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
          <div style={{marginTop:16,display:'flex',alignItems:'center',gap:12,justifyContent:'space-between',flexWrap:'wrap'}}>
            <div style={{fontSize:12,color:allDone?'#107C10':'#94a3b8',fontWeight:allDone?700:400}}>
              {allDone?'✓ Mandatory tables uploaded — ready to build':`${tables.filter(t=>tableStatus[t.name]==='done').length} / ${tables.length} tables uploaded`}
            </div>
            <div style={{display:'flex',alignItems:'center',gap:10,flexWrap:'wrap'}}>
              {buildMsg&&<div style={{fontSize:12,color:buildMsg.startsWith('Error')?'#D13438':'#107C10',fontWeight:600}}>{buildMsg}</div>}
              {buildMsg&&!buildMsg.startsWith('Error')&&(
                <button onClick={()=>{const url=`${API}/p2p/download_output?username=${encodeURIComponent(currentUser||'Unknown')}`;const a=document.createElement('a');a.href=url;a.download='';a.click();}}
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
                {building?'⏳ Building…':'Build Event Log'}
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


const UploadBanner=React.memo(({onUploaded,serverOk,onLoadingChange,currentUser,myFiles,fetchingFiles,handleLoadOldFile})=>{
  // 'info' = landing info page, 'table' = SAP table upload, 'upload' = pre-built CSV
  const [step,setStep]=useState('info');
  const [dragging,setDragging]=useState(false);
  const [status,setStatus]=useState('idle');
  const [msg,setMsg]=useState('');
  const [selectedFile, setSelectedFile] = useState(null);
  const [colMapping, setColMapping] = useState(null);
  const inputRef=useRef();

  const doUpload=async(file, mapping={})=>{
    if(!file) return;
    if(!file.name.toLowerCase().endsWith('.csv')){setStatus('error');setMsg('Only .csv files accepted.');return;}
    setStatus('uploading');setMsg('');
    setSelectedFile(file);
    onLoadingChange(true,10,'Processing Data...');
    const form=new FormData();
    form.append('file',file);
    form.append('username',currentUser);
    form.append('column_mapping', JSON.stringify(mapping));
    let prog=10;
    const ticker=setInterval(()=>{prog=Math.min(prog+Math.random()*12,88);onLoadingChange(true,prog,'Analysing Data...');},400);
    try{
      const r=await fetch(`${API}/p2p/upload`,{method:'POST',body:form});
      const d=await r.json();
      clearInterval(ticker);
      if(!r.ok) throw new Error(d.detail||`HTTP ${r.status}`);
      onLoadingChange(true,100,'Dashboard Created');
      setTimeout(()=>{setStatus('done');setMsg(`✓ ${Number(d.rows).toLocaleString()} rows · ${Number(d.unique_cases).toLocaleString()} unique cases`);onUploaded();},800);
    }catch(e){
      clearInterval(ticker);onLoadingChange(false,0,'');
      if (e.message.includes('Column mapping is incorrect')) {
        onUploaded();
      } else {
        setStatus('error');setMsg(`Error: ${e.message}`);
      }
    }finally{
      if(inputRef.current) inputRef.current.value='';
    }
  };

  /* ── Page 1: Info / landing ── */
  if(step==='info') return(
    <P2PIntroScreen
      onGoTableBuild={()=>setStep('table')}
      onGoCsvUpload={()=>setStep('upload')}
      currentUser={currentUser}
    />
  );

  /* ── Page 2: SAP Table upload → Build Event Log ── */
  if(step==='table') return(
    <TableUploadScreen
      onBuilt={onUploaded}
      onBack={()=>setStep('info')}
      onLoadingChange={onLoadingChange}
      currentUser={currentUser}
      myFiles={myFiles}
      fetchingFiles={fetchingFiles}
      handleLoadOldFile={handleLoadOldFile}
    />
  );

  /* ── Page 3: Pre-built CSV upload ── */
  if(step==='upload'){
    const bc=dragging?C.blue700:status==='done'?'#107C10':status==='error'?C.red:C.border;
    const bg=dragging?'#EFF6FF':status==='done'?'#F0FAF0':status==='error'?'#FDE7E9':'#FAFAFA';
    const SCHEMA_COLS=[
      {col:'UniqueID_PO',               desc:'Unique PO line key (EBELN + EBELP)',       req:true},
      {col:'PO Creation',               desc:'PO creation date (AEDAT from EKKO)',        req:true},
      {col:'PO Date',                   desc:'PO document date (BEDAT from EKKO)',         req:true},
      {col:'GR Posting',                desc:'Goods Receipt posting date (BUDAT/EKBE)',   req:true},
      {col:'Invoice Posting',           desc:'Invoice posting date (BUDAT/EKBE)',          req:true},
      {col:'PR Creation',               desc:'PR requirement date (BADAT from EBAN)',      req:false},
      {col:'PR Release Date',           desc:'PR release date (FRGDT from EBAN)',          req:false},
      {col:'PR Reversal Date',          desc:'PR reversal date',                           req:false},
      {col:'PO Reversal Date',          desc:'PO reversal date',                           req:false},
      {col:'GR Reversal Date',          desc:'GR reversal date',                           req:false},
      {col:'Invoice Reversal Date',     desc:'Invoice reversal date',                     req:false},
      {col:'BUKRS',                     desc:'Company code',                               req:false},
      {col:'LIFNR',                     desc:'Vendor ID',                                  req:false},
      {col:'NAME1',                     desc:'Vendor name (from LFA1)',                   req:false},
      {col:'BSART',                     desc:'Document type',                              req:false},
      {col:'MATKL',                     desc:'Material group',                             req:false},
      {col:'EKGRP',                     desc:'Purchasing group',                           req:false},
      {col:'WERKS',                     desc:'Plant',                                      req:false},
      {col:'ERNAM',                     desc:'PO creator (ERNAM from EKKO)',              req:false},
      {col:'ERNAM (EBAN)',              desc:'PR creator (ERNAM from EBAN)',              req:false},
      {col:'GR Creation User',          desc:'GR posting user',                           req:false},
      {col:'Invoice Creation User',     desc:'Invoice posting user',                      req:false},
    ];
    const csvUploads=(myFiles||[]).filter(f=>!f.source||f.source==='csv_upload');

    const handleMapCsvColumns = async () => {
      if (!selectedFile) return;
      const formPreview=new FormData();
      formPreview.append('file',selectedFile);
      try{
        const rPrev=await fetch(`${API}/p2p/transform/preview_columns`,{method:'POST',body:formPreview});
        const dPrev=await rPrev.json();
        if(!rPrev.ok) throw new Error(dPrev.detail||`Failed to read CSV columns`);
        setColMapping({ file: selectedFile, tableDef: { name: 'Pre-built CSV', required: SCHEMA_COLS.map(c=>({ col: c.col, note: c.desc })) }, uploadedCols: dPrev.columns, mapping: {} });
      }catch(e){
        setStatus('error');
        setMsg(e.message);
      }
    };

    return(
      <div style={{display:'flex',flexDirection:'column',gap:16,padding:'20px 14px'}}>
        {/* ══ Column Mapping Modal (CSV) ══════════════════════════════════════════════ */}
        {colMapping&&(
          <div style={{position:'fixed',inset:0,zIndex:9999,background:'rgba(0,0,0,0.5)',
            display:'flex',alignItems:'center',justifyContent:'center',padding:16}}
            onClick={()=>setColMapping(null)}>
            <div style={{background:'#fff',borderRadius:12,width:'100%',maxWidth:700,
              maxHeight:'88vh',display:'flex',flexDirection:'column',
              boxShadow:'0 24px 64px rgba(0,0,0,0.35)'}}
              onClick={e=>e.stopPropagation()}>
              <div style={{padding:'20px 24px 16px',borderBottom:'1px solid #E2E8F0',flexShrink:0}}>
                <div style={{fontSize:10,fontWeight:700,color:'#DC2626',textTransform:'uppercase',letterSpacing:1,marginBottom:4}}>Column Mapping</div>
                <div style={{fontSize:17,fontWeight:700,color:'#1e293b'}}>Map Columns for {colMapping.tableDef.name}</div>
                <div style={{fontSize:12,color:'#64748b',marginTop:3}}> Select which columns from your file correspond to the required/optional fields.</div>
              </div>
              
              <div style={{overflowY:'auto',flex:1,padding:'0 0 8px'}}>
                <table style={{width:'100%',borderCollapse:'collapse',fontSize:12}}>
                  <thead style={{position:'sticky',top:0,zIndex:2}}>
                    <tr style={{background:'#F8FAFC'}}>
                      <th style={{padding:'10px 16px',textAlign:'left',fontWeight:700,color:'#64748b',fontSize:10,textTransform:'uppercase',letterSpacing:0.8,borderBottom:'2px solid #E2E8F0',width:'35%'}}>Column</th>
                      <th style={{padding:'10px 12px',textAlign:'left',fontWeight:700,color:'#64748b',fontSize:10,textTransform:'uppercase',letterSpacing:0.8,borderBottom:'2px solid #E2E8F0',width:'35%'}}>Map to File Column</th>
                      <th style={{padding:'10px 12px',textAlign:'left',fontWeight:700,color:'#64748b',fontSize:10,textTransform:'uppercase',letterSpacing:0.8,borderBottom:'2px solid #E2E8F0'}}>Purpose</th>
                    </tr>
                  </thead>
                  <tbody>
                    {colMapping.tableDef.required.map((r,i)=>{
                      const reqCol = r.col;
                      const autoMatch = colMapping.uploadedCols.find(c=>c.toUpperCase()===reqCol.toUpperCase());
                      const selected = colMapping.mapping[reqCol] !== undefined ? colMapping.mapping[reqCol] : (autoMatch || '');
                      return(
                        <tr key={reqCol} style={{borderBottom:'1px solid #F1F5F9'}}>
                          <td style={{padding:'10px 16px',fontFamily:'monospace',fontWeight:700,color:'#334155',fontSize:13}}>{reqCol}</td>
                          <td style={{padding:'10px 12px'}}>
                            <select 
                              value={selected} 
                              onChange={e=>setColMapping(p=>({...p, mapping:{...p.mapping, [reqCol]: e.target.value}}))}
                              style={{width:'100%',padding:'6px 8px',borderRadius:4,border:'1px solid #CBD5E1',background:'#fff',fontSize:12,color:'#334155'}}
                            >
                              <option value="">-- Leave Blank / Unmapped --</option>
                              {colMapping.uploadedCols.map(c=>(
                                <option key={c} value={c}>{c}</option>
                              ))}
                            </select>
                          </td>
                          <td style={{padding:'10px 12px',color:'#64748b',fontSize:11,lineHeight:1.4}}>{r.note}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>

              <div style={{padding:'14px 20px',borderTop:'1px solid #E2E8F0',flexShrink:0,display:'flex',justifyContent:'flex-end',gap:12}}>
                <button onClick={()=>setColMapping(null)}
                  style={{padding:'8px 16px',background:'#fff',color:'#64748b',border:'1px solid #CBD5E1',borderRadius:6,fontSize:13,fontWeight:700,cursor:'pointer'}}>
                  Cancel
                </button>
                <button 
                  onClick={()=>{
                    const finalMapping = {};
                    colMapping.tableDef.required.forEach(r => {
                        const autoMatch = colMapping.uploadedCols.find(c=>c.toUpperCase()===r.col.toUpperCase());
                        const sel = colMapping.mapping[r.col] !== undefined ? colMapping.mapping[r.col] : (autoMatch || '');
                        if (sel && sel !== r.col) {
                            finalMapping[sel] = r.col;
                        }
                    });
                    setColMapping(null);
                    doUpload(colMapping.file, finalMapping);
                  }}
                  style={{padding:'8px 16px',background:'#0078D4',color:'#fff',border:'none',borderRadius:6,fontSize:13,fontWeight:700,cursor:'pointer'}}>
                  Confirm Mapping & Upload
                </button>
              </div>
            </div>
          </div>
        )}
        {/* Back + status */}
        <div style={{display:'flex',alignItems:'center',gap:10}}>
          <button onClick={()=>setStep('info')}
            style={{background:'none',border:'1px solid #E2E8F0',padding:'5px 12px',borderRadius:6,
              fontSize:12,cursor:'pointer',color:'#64748b',fontWeight:600,flexShrink:0}}
            onMouseOver={e=>e.currentTarget.style.background='#F8FAFC'}
            onMouseOut={e=>e.currentTarget.style.background='none'}>
            ← Back
          </button>
          <div style={{fontSize:11,fontWeight:700,color:'#0078D4',textTransform:'uppercase',letterSpacing:0.8}}>
            Upload Pre-built CSV
          </div>
          <div style={{display:'flex',alignItems:'center',gap:5,fontSize:11,color:C.slate,marginLeft:'auto'}}>
            <div style={{width:7,height:7,borderRadius:'50%',background:serverOk?'#107C10':'#D13438',
              boxShadow:serverOk?'0 0 0 2px rgba(16,124,16,.2)':'0 0 0 2px rgba(209,52,56,.2)'}}/>
            {serverOk?'Backend connected':'Backend offline'}
          </div>
        </div>


        {/* Drop zone */}
        <div onDragOver={e=>{e.preventDefault();setDragging(true);}}
          onDragLeave={()=>setDragging(false)}
          onDrop={e=>{e.preventDefault();setDragging(false);const f=e.dataTransfer.files[0]; if(f){setSelectedFile(f);setStatus('idle');setMsg('');}}}
          onClick={()=>{if(status!=='uploading'&&inputRef.current){inputRef.current.value='';inputRef.current.click();}}}
          style={{border:`2px dashed ${bc}`,borderRadius:8,padding:'14px 24px',background:bg,
            cursor:'pointer',textAlign:'center',transition:'all .2s',
            display:'flex',alignItems:'center',justifyContent:'center',gap:14,flexDirection:selectedFile?'column':'row'}}>
          <input ref={inputRef} type="file" accept=".csv" style={{display:'none'}} onChange={e=>{const f=e.target.files[0]; if(f){setSelectedFile(f);setStatus('idle');setMsg('');}}}/>
          
          <div style={{display:'flex',alignItems:'center',gap:14}}>
            <div style={{fontSize:22,fontWeight:'bold',color:status==='done'?'#107C10':status==='error'?'#D13438':'#0078D4'}}>
              {status==='done'?'✓':status==='error'?'✕':'⬆'}
            </div>
            <div style={{textAlign:'left'}}>
              <div style={{fontSize:13,fontWeight:700,color:'#323130'}}>
                {selectedFile?selectedFile.name:status==='idle'?'Click or drag & drop a CSV file here':status==='done'?'File loaded!':'Upload failed'}
              </div>
              <div style={{fontSize:11,color:C.slate,marginTop:2}}>{msg||'Wide-format or KNIME long-format CSV accepted'}</div>
            </div>
          </div>

          {selectedFile && status !== 'uploading' && (
            <div style={{display: 'flex', gap: 12, marginTop: 4}}>
              <button onClick={e=>{e.stopPropagation(); handleMapCsvColumns();}}
                style={{fontSize:12,padding:'8px 16px',background:'#EFF6FF',color:'#1D4ED8',
                  border:'1px solid #93C5FD',borderRadius:6,cursor:'pointer',fontWeight:700}}
                onMouseOver={e=>e.currentTarget.style.background='#DBEAFE'}
                onMouseOut={e=>e.currentTarget.style.background='#EFF6FF'}>
                Map Columns
              </button>
              <button onClick={e=>{e.stopPropagation(); doUpload(selectedFile, {});}}
                style={{fontSize:12,padding:'8px 16px',background:'#0078D4',color:'#fff',
                  border:'none',borderRadius:6,cursor:'pointer',fontWeight:700}}
                onMouseOver={e=>e.currentTarget.style.background='#005A9E'}
                onMouseOut={e=>e.currentTarget.style.background='#0078D4'}>
                Upload
              </button>
              <button onClick={e=>{e.stopPropagation();setStatus('idle');setMsg('');setSelectedFile(null);}}
                style={{fontSize:12,padding:'8px 16px',background:'#fff',
                  border:`1px solid ${C.border}`,borderRadius:6,cursor:'pointer',color:C.slate}}>
                Clear
              </button>
            </div>
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
});


const EmptyState = ({ condition, message, children, action }) => {
  if (!condition) return children;
  return (
    <div style={{display:'flex', alignItems:'center', justifyContent:'center', height:'300px', 
      background:'#F8FAFC', borderRadius:'6px', border:'1px dashed #CBD5E1', color:'#64748b', fontSize:'13px', padding:'20px', textAlign:'center', flexDirection:'column', gap:'12px'}}>
      <div style={{fontSize:'28px', opacity:0.8}}>📉</div>
      <div style={{fontWeight:600, maxWidth:'250px'}}>{message}</div>
      {action}
    </div>
  );
};

/* ════════════════════════════════════════════
   MAIN APP
════════════════════════════════════════════ */
export default function P2PDashboard({ currentUser, onSignOut, onBackHome }){
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
    company:'ALL',bsart:'ALL',matkl:'ALL',vendor:'ALL',
    purch_group:'ALL',case_id:'ALL',
    month:'ALL',year:'ALL',quarter:'ALL',lifnr:'ALL', lead_time: 'ALL', ernam: 'ALL', status: 'ALL'
  });
  const [crossFilter,setCrossFilter]=useState(null);
  const [hoverInfo,  setHoverInfo]  =useState(null);
  
  const [kpis,      setKpis]      =useState(null);
  const [actData,   setActData]   =useState([]);
  const [monData,   setMonData]   =useState([]);
  const [compData,  setCompData]  =useState([]);
  const [bsData,    setBsData]    =useState([]);
  const [mkData,    setMkData]    =useState([]);
  const [vendData,  setVendData]  =useState([]);
  const [ltData,    setLtData]    =useState([]);
  const [ernamData, setErnamData] =useState([]); 
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
      fetch(`${API}/p2p/my_files?username=${currentUser}`)
        .then(res => res.ok ? res.json() : [])
        .then(data => setMyFiles(data))
        .catch(err => console.error("Failed to fetch files", err))
        .finally(() => setFetchingFiles(false));
    }
  }, [currentUser, dataLoaded, refreshTrigger]);

  const handleLoadOldFile = async (file_id) => {
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
    } catch(e) {
      alert("Error loading dashboard: " + e.message);
    } finally {
      setTimeout(() => handleLoadingChange(false, 100, ''), 500);
    }
  };

  const logAction = useCallback((action, details) => {
    if (!currentUser) return;
    fetch(`${API}/p2p/log`, {
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
    try { await fetch(`${API}/p2p/clear`, { method: 'POST' }); } catch(e){}
    intentToUpload.current = true;
    setDataLoaded(false);
    setChartsReady(false);
    setPmReady(false);
    setKpis(null);
    if (onSignOut) onSignOut();
  };

  const handleResetData = () => {
    logAction('RESET_DATA', 'Started upload new file flow');
    intentToUpload.current = true; 
    setDataLoaded(false);
    setChartsReady(false);
    setPmReady(false);
    setKpis(null);
    setActData([]);
    setCaseTableData([]);
    setCaseEvents([]);
    setErnamData([]);
    setSelected({
      company:'ALL',bsart:'ALL',matkl:'ALL',vendor:'ALL',
      purch_group:'ALL',case_id:'ALL',
      month:'ALL',year:'ALL',quarter:'ALL',lifnr:'ALL', lead_time: 'ALL', ernam: 'ALL', status: 'ALL'
    });
    setCrossFilter(null);
  };
  
  const handleRefresh = () => {
    logAction('REFRESH', 'Refreshed the dashboard');
    setRefreshTrigger(p => p + 1);
  };

  useEffect(()=>{
    if (!currentUser) return;
    const ping=()=>fetch(`${API}/`).then(r=>r.ok?r.json():null)
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
    fetch(`${API}/p2p/filters${baseQStr()}`)
      .then(r=>r.ok?r.json():{})
      .then(d=>setFilters(d&&typeof d==='object'&&!Array.isArray(d)?d:{}))
      .catch(()=>setFilters({}));
  },[baseQStr,dataLoaded, refreshTrigger]);

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

  useEffect(()=>{
    if(!dataLoaded) return;
    if (chartsReady) setDashboardLoading(true); 
    
    const cq=effectiveQStr();
    
    const arr=(u,s)=>fetch(u).then(r=>r.ok?r.json():[]).then(d=>s(Array.isArray(d)?d:[])).catch(()=>s([]));
    const obj=(u,s)=>fetch(u).then(r=>r.ok?r.json():null).then(d=>s(d&&typeof d==='object'&&!Array.isArray(d)?d:null)).catch(()=>s(null));

    const promises = [
      fetch(`${API}/p2p/kpis${cq}`).then(r=>{if(!r.ok)return{total_cases:0};return r.json();}).then(d=>setKpis(d)).catch(()=>setKpis({total_cases:0})),
      arr(`${API}/p2p/charts/activity${cq}`, setActData),
      arr(`${API}/p2p/charts/monthly${cq}`,  setMonData),
      arr(`${API}/p2p/charts/company${cq}`,  setCompData),
      arr(`${API}/p2p/charts/bsart${cq}`,    setBsData),
      arr(`${API}/p2p/charts/matkl${cq}`,    setMkData),
      arr(`${API}/p2p/charts/vendors${cq}`,  setVendData),
      arr(`${API}/p2p/charts/leadtime${cq}`, setLtData),
      arr(`${API}/p2p/charts/ernam${cq}`,    setErnamData),
      arr(`${API}/p2p/cases${cq}`,           setCaseTableData),
      
      arr(`${API}/p2p/charts/po_rev_ernam${cq}`, setPoRevErnam),
      arr(`${API}/p2p/charts/po_rev_timeline${cq}`, setPoRevTimeline),
      arr(`${API}/p2p/charts/pr_rev_after_po_ernam${cq}`, setPrRevAfterPo),
      arr(`${API}/p2p/charts/seq_violation_ernam${cq}`, setSeqViolation),
      arr(`${API}/p2p/charts/happy_path${cq}`, setHappyPathData),
      arr(`${API}/p2p/charts/sod_violations${cq}`, setSodData),
      arr(`${API}/p2p/charts/bottleneck${cq}`, setBottleneckData),
      arr(`${API}/p2p/charts/rev_by_purch_group${cq}`, setRevByPurchGroup),
      arr(`${API}/p2p/charts/purch_group_workload${cq}`, setPurchGroupWorkload),
      arr(`${API}/p2p/charts/vendor_lead_time${cq}`, setVendorLeadTime),
    ];

    Promise.all(promises).finally(() => {
      setDashboardLoading(false); 
      setChartsReady(true);
    });

  },[effectiveQStr,dataLoaded, refreshTrigger]);

  useEffect(()=>{
    if(!dataLoaded) return;
    const qStr=effectiveQStr();
    
    if (pmReady) setPmLoading(true);
    setPmError('');
    
    fetch(`${API}/p2p/process-map${qStr}`)
      .then(r=>{if(!r.ok) throw new Error(`HTTP ${r.status}`);return r.json();})
      .then(d=>{
        setRawGraphData(d);
        buildFlowMap(d.nodes, d.edges, setRfNodes, setRfEdges, layoutDir);
      })
      .catch(err=>{setPmError(`Failed: ${err.message}`);})
      .finally(() => {
        setPmLoading(false);
        setPmReady(true);
      });
  },[effectiveQStr,dataLoaded, refreshTrigger]);

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

  const slicer=(key,label,filterKey,wide=false)=>{
    const raw=filters[filterKey];
    const opts=Array.isArray(raw)?raw:['ALL'];
    const deduped=opts[0]==='ALL'?opts:['ALL',...opts];
    
    const handleSlicerChange = (val) => {
        logAction('FILTER', `Changed slicer ${key} to ${val}`);
        setSelected(prev=>({...prev,[key]:val}));
        setCrossFilter(null);
    };

    if(key === 'case_id' || key === 'vendor' || key === 'lifnr') {
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
    setSelected({company:'ALL',bsart:'ALL',matkl:'ALL',vendor:'ALL',
      purch_group:'ALL',case_id:'ALL',
      month:'ALL',year:'ALL',quarter:'ALL',lifnr:'ALL', lead_time:'ALL', ernam: 'ALL', status: 'ALL'});
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
            <div style={{fontWeight:700,fontSize:16,color:'#fff'}}>P2P Process Explorer</div>
            <div style={{fontSize:10,color:'rgba(255,255,255,.5)'}}>Procure-to-Pay Process Mining</div>
          </div>
          {crossFilter&&(
            <div style={{display:'flex',alignItems:'center',gap:6,marginLeft:16,
              background:'rgba(255,255,255,.12)',border:'1px solid rgba(255,255,255,.2)',
              borderRadius:6,padding:'4px 12px',fontSize:12}}>
              <span style={{color:'#fff',fontWeight:600}}>Filter: {crossFilter.type}: <strong>{crossFilter.value}</strong></span>
              <button onClick={clearCF} style={{background:'none',border:'none',cursor:'pointer',
                color:'rgba(255,255,255,.8)',fontWeight:700,fontSize:14,padding:'0 2px'}}>X</button>
            </div>
          )}
        </div>
        
        <div style={{display:'flex', alignItems:'center', gap:12}}>
          {dataLoaded&&kpis&&(
            <div style={{fontSize:11,color:'rgba(255,255,255,.5)'}}>
              {Number(kpis.total_cases).toLocaleString()} cases loaded
            </div>
          )}

          {dataLoaded && (
            <div style={{display:'flex', alignItems:'stretch', gap:0,
              background:'rgba(255,255,255,0.08)', borderRadius:6,
              border:'1px solid rgba(255,255,255,0.15)', overflow:'hidden'}}>
              <button
                className={`tab-button ${activeTab === 'process' ? 'active' : ''}`}
                onClick={() => { logAction('TAB', 'Viewed Process Mining'); setActiveTab('process'); }}
              >
                Process Mining
              </button>
              <button
                className={`tab-button ${activeTab === 'dimensions' ? 'active' : ''}`}
                onClick={() => { logAction('TAB', 'Viewed Dimensions'); setActiveTab('dimensions'); }}
              >
                EDA
              </button>
              <button
                onClick={handleResetData}
                style={{
                    fontSize: 11, fontWeight: 600,
                    background: 'transparent',
                    color: 'rgba(255,255,255,0.75)', border: 'none',
                    borderLeft: '1px solid rgba(255,255,255,0.15)',
                    padding: '8px 14px', cursor: 'pointer',
                    transition: 'all 0.2s', whiteSpace:'nowrap'
                }}
                onMouseOver={e => { e.currentTarget.style.background = 'rgba(255,255,255,0.12)'; e.currentTarget.style.color='#fff'; }}
                onMouseOut={e => { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.color='rgba(255,255,255,0.75)'; }}
              >
                📂 Upload New File
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
               }} 
               serverOk={serverOk} 
               onLoadingChange={handleLoadingChange}
               myFiles={myFiles}
               fetchingFiles={fetchingFiles}
               handleLoadOldFile={handleLoadOldFile}/>
        )}

        {dataLoaded && kpis && kpis.total_cases === 0 && (
          <EmptyState condition={true} message="Column mapping is not correct." action={
            <button onClick={handleResetData} style={{padding:'8px 16px',background:'#0078D4',color:'#fff',border:'none',borderRadius:6,fontWeight:700,cursor:'pointer'}}>Fix Mapping / Upload New File</button>
          } />
        )}
        {dataLoaded && !(kpis && kpis.total_cases === 0) && (<>
          <div style={{background:C.card,borderRadius:8,padding:'10px 14px',
            border:`1px solid ${C.border}`,boxShadow:'0 2px 6px rgba(0,0,0,.04)'}}>
            
            <div style={{
               display: 'grid', 
               gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', 
               gap: '12px', 
               alignItems: 'end'
            }}>
              {slicer('case_id',    'Case ID',    'case_ids',true)}
              {slicer('company',    'Company',    'companies')}
              {slicer('matkl',      'Material Group', 'matkls')}
              {slicer('vendor',     'Vendor',     'vendors',true)}
              {slicer('purch_group','Purchasing Group', 'purch_groups')}
              {slicer('lifnr',      'Vendor ID',  'lifnrs')}
              {slicer('year',       'Year',       'years')}
              
              <div style={{display:'flex', gap:8}}>
                <button onClick={resetAll} style={{
                    padding:'6px 12px', fontSize:12, fontWeight:700,
                    background:'#F3F2F1', color:'#323130', border:`1px solid #D2D0CE`,
                    borderRadius:4, cursor:'pointer', height: '28px', flex:1
                }}>
                    Reset
                </button>
                <button onClick={handleRefresh} style={{
                    padding:'6px 12px', fontSize:12, fontWeight:700,
                    background:'#0078D4', color:'#fff', border:'none',
                    borderRadius:4, cursor:'pointer', height: '28px', flex:1,
                    display:'flex',alignItems:'center',justifyContent:'center'
                }}>
                    🔄
                </button>
                <button
                  onClick={() => {
                    const url = `${API}/p2p/download_output?username=${encodeURIComponent(currentUser || 'Unknown')}`;
                    const a = document.createElement('a');
                    a.href = url; a.download = ''; a.click();
                    logAction('DOWNLOAD', 'Downloaded output CSV from dashboard');
                  }}
                  title="Download current dataset as CSV (also saved to P2P_Output folder)"
                  style={{
                    padding:'6px 10px', fontSize:12, fontWeight:700,
                    background:'#006B3C', color:'#fff', border:'none',
                    borderRadius:4, cursor:'pointer', height:'28px', flex:1,
                    display:'flex', alignItems:'center', justifyContent:'center', gap:4,
                    whiteSpace:'nowrap'
                  }}
                  onMouseOver={e => e.currentTarget.style.background='#004d2c'}
                  onMouseOut={e => e.currentTarget.style.background='#006B3C'}
                >
                  ⬇ CSV
                </button>
              </div>
            </div>
          </div>

          {kpis&&(<>
            <EmptyState condition={kpis.total_cases === 0} message="No valid cases found. The column mapping may be incorrect. Please check your mapping and rebuild the event log.">
              <div style={{display:'grid',gridTemplateColumns:'repeat(7,1fr)',gap:8}}>
              <KpiCard label="Cases"                 value={kpis.total_cases}          color="#6a3382" tooltip={kpiTooltips.total_cases}/>
              <KpiCard label="PO Created"            value={kpis.po_created}           color="#6a3382" tooltip={kpiTooltips.po_created}/>
              <KpiCard label="GR Postings"           value={kpis.gr_postings}          color="#6a3382" tooltip={kpiTooltips.gr_postings}/>
              <KpiCard label="Invoices Posted"       value={kpis.invoices_posted}      color="#6a3382" tooltip={kpiTooltips.invoices_posted}/>
              <KpiCard label="Total Reversals"       value={kpis.reversals}            color="#6a3382" tooltip={kpiTooltips.reversals}/>
              <KpiCard label="Vendors"               value={kpis.unique_vendors}       color="#6a3382" tooltip={kpiTooltips.unique_vendors}/>
              <KpiCard label="Avg Completion (Days)" value={kpis.avg_completion_days}  color="#6a3382" tooltip="Average end-to-end process duration in days"/>
            </div>
            
            <div style={{display:'grid',gridTemplateColumns:'repeat(5,1fr)',gap:8}}>
              <ConfKpiCard label="PO Without PR"         value={kpis.po_without_pr}   color="#6a3382" sub="PO raised without PR"   tooltip={kpiTooltips.po_without_pr}/>
              <ConfKpiCard label="PR Rev. After PO Date" value={kpis.pr_rev_after_po} color="#6a3382" sub="Late PR reversals"       tooltip={kpiTooltips.pr_rev_after_po}/>
              <ConfKpiCard label="PO Rev. After GR"      value={kpis.po_rev_after_gr} color="#6a3382" sub="Late PO reversals"       tooltip={kpiTooltips.po_rev_after_gr}/>
              <ConfKpiCard label="GR Without Invoice"    value={kpis.gr_no_invoice}   color="#6a3382" sub="GR not yet invoiced"     tooltip={kpiTooltips.gr_no_invoice}/>
              <ConfKpiCard label="Invoice Without GR"    value={kpis.inv_no_gr}       color="#6a3382" sub="Invoice before GR"       tooltip={kpiTooltips.inv_no_gr}/>
            </div>
            </EmptyState>
          </>)}

          {activeTab === 'process' && (
            <div style={{display:'flex', flexDirection:'column', gap:10, flex:1, paddingBottom: '20px'}}>
              <div style={{display:'grid', gridTemplateColumns:'1fr 1fr', gap:10}}>
                <div style={{display:'flex', flexDirection:'column', gap:10}}>

                  <div style={{background:C.card,borderRadius:8,border:`1px solid ${C.border}`,
                    boxShadow:'0 2px 8px rgba(0,0,0,.05)',overflow:'hidden',
                    display:'flex',flexDirection:'column', height:915}}>

                  <div style={{padding:'12px 14px 8px',borderBottom:`1px solid ${C.border}`,
                    background: C.jkBlue, 
                    display:'flex',justifyContent:'space-between',alignItems:'center',flexShrink:0}}>
                    <div>
                      <div style={{fontSize:13,fontWeight:700,color:'#fff'}}>Process Map</div>
                      <div style={{fontSize:10,color:'rgba(255,255,255,0.7)'}}>Flow & Frequency Analysis</div>
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
                        <Background color="#C8D3E8" gap={24} size={1} variant="dots"/>
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
                        <div style={{fontWeight:700,color:'#0078D4',marginBottom:6}}>{hoverInfo.title}</div>
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

                  <ChartCard title="Segregation of Duties (SoD) Violations" subtitle="Same user performed conflicting activities — click a bar to filter all charts" loading={dashboardLoading} highlighted={crossFilter?.type==='sod'} onClear={clearCF}>
                    <SodViolationsChart data={sodData} crossFilter={crossFilter} onSelect={handleSelect} />
                  </ChartCard>

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

                  <ChartCard title="Lead Time Distribution" 
                    loading={dashboardLoading}
                    highlighted={crossFilter?.type==='lead_time'} onClear={clearCF}>
                    <LeadTimeChart data={ltData} crossFilter={crossFilter} onSelect={handleSelect}/>
                  </ChartCard>

                  <ChartCard title="Average days between each process step" loading={dashboardLoading}>
                    <BottleneckChart data={bottleneckData} />
                  </ChartCard>
                </div>
              </div>

              {/* --- ANOMALIES & DEVIATIONS — 2-col grid --- */}
              <div style={{marginTop: '2px'}}>
                <div style={{display:'grid', gridTemplateColumns:'repeat(2, 1fr)', gap:10}}>
                  
                  <ChartCard title="PO Reversals by ERNAM" subtitle="Users reversing the most POs" loading={dashboardLoading} highlighted={crossFilter?.type==='ernam'} onClear={clearCF}>
                    <EmptyState condition={!dashboardLoading && poRevErnam.length === 0} message="No PO reversals found.">
                      <ScrollableHBarChart data={poRevErnam} dataKey="count" labelKey="ernam" color="#5aabee" crossFilter={crossFilter} crossKey="ernam" onSelect={handleSelect}/>
                    </EmptyState>
                  </ChartCard>

                  <ChartCard title="PO Reversals Timeline" subtitle="Trend of PO reversals over time" loading={dashboardLoading}>
                    <EmptyState condition={!dashboardLoading && poRevTimeline.length === 0} message="No PO reversals found.">
                      <MonthlyChart data={poRevTimeline} crossFilter={crossFilter} onSelect={handleSelect}/>
                    </EmptyState>
                  </ChartCard>

                  <ChartCard title="Late PR Reversals (By ERNAM)" subtitle="PRs reversed AFTER PO creation" loading={dashboardLoading} highlighted={crossFilter?.type==='ernam'} onClear={clearCF}>
                    <EmptyState condition={!dashboardLoading && prRevAfterPo.length === 0} message="No late PR reversals found (or Requisition data is not uploaded).">
                      <ScrollableHBarChart data={prRevAfterPo} dataKey="count" labelKey="ernam" color="#CA5010" crossFilter={crossFilter} crossKey="ernam" onSelect={handleSelect}/>
                    </EmptyState>
                  </ChartCard>

                  <ChartCard title="PO created AFTER GR or Invoice (By ERNAM)" loading={dashboardLoading} highlighted={crossFilter?.type==='ernam'} onClear={clearCF}>
                    <EmptyState condition={!dashboardLoading && seqViolation.length === 0} message="No sequence violations found (or GR/Invoice data is not uploaded).">
                      <ScrollableHBarChart data={seqViolation} dataKey="count" labelKey="ernam" color="#1aca60" crossFilter={crossFilter} crossKey="ernam" onSelect={handleSelect}/>
                    </EmptyState>
                  </ChartCard>

                </div>
              </div>

              {/* --- PO Reversals by Purchasing Group — full width vertical scrollable bar --- */}
              <div style={{marginTop: '2px'}}>
                <ChartCard title="PO Reversals by Purchasing Group" subtitle="Purchasing groups with the most PO reversal activity — click a bar to filter" loading={dashboardLoading} highlighted={crossFilter?.type==='purch_group'} onClear={clearCF}>
                  <EmptyState condition={!dashboardLoading && revByPurchGroup.length === 0} message="No PO reversals found.">
                    <RevByPurchGroupVBarChart data={revByPurchGroup} crossFilter={crossFilter} onSelect={handleSelect} />
                  </EmptyState>
                </ChartCard>
              </div>
              
              <div style={{display:'grid', gridTemplateColumns:'1fr', gap:10, marginTop: '10px'}}>
                  <ChartCard title="Case Details" subtitle="Click a Case ID to view its chronological event log" loading={dashboardLoading}>
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
              
              <ChartCard title="Monthly Trend (Unique Cases)" subtitle="Unique active cases per month"
                loading={dashboardLoading}
                highlighted={crossFilter?.type==='month'} onClear={clearCF}>
                <MonthlyChart data={monData} crossFilter={crossFilter} onSelect={handleSelect}/>
              </ChartCard>

              <ChartCard title="User Activity (ERNAM)" subtitle="Who performed activities (Unique Cases)" 
                loading={dashboardLoading}
                highlighted={crossFilter?.type==='ernam'} onClear={clearCF}>
                <ErnamChart data={ernamData} crossFilter={crossFilter} onSelect={handleSelect}/>
              </ChartCard>
              
              <ChartCard title="Company Distribution" subtitle="Cases per company code"
                loading={dashboardLoading}
                highlighted={crossFilter?.type==='company'} onClear={clearCF}>
                <CompanyDonutChart data={compData} crossFilter={crossFilter} onSelect={handleSelect}/>
              </ChartCard>

              <ChartCard title="PO Type (BSART)" subtitle="Distribution by document type"
                loading={dashboardLoading}
                highlighted={crossFilter?.type==='bsart'} onClear={clearCF}>
                <ScrollableVBarChart data={bsData} crossFilter={crossFilter} onSelect={handleSelect}/>
              </ChartCard>

              <ChartCard title="Material Group" subtitle="Purchasing activity by material category"
                loading={dashboardLoading}
                highlighted={crossFilter?.type==='matkl'} onClear={clearCF}>
                <ScrollableHBarChart data={mkData} dataKey="count" labelKey="matkl"
                  color="#038387" crossFilter={crossFilter} crossKey="matkl" onSelect={handleSelect}/>
              </ChartCard>

              <ChartCard title="Vendors by Cases" subtitle="Top suppliers by volume"
                loading={dashboardLoading}
                highlighted={crossFilter?.type==='vendor'} onClear={clearCF}>
                <ScrollableHBarChart data={vendData} dataKey="count" labelKey="vendor"
                  color="#5C2D91" crossFilter={crossFilter} crossKey="vendor" onSelect={handleSelect}/>
              </ChartCard>

              <ChartCard title="Purchasing Group Workload (EKGRP)" subtitle="Cases handled per purchasing group — click a bar to filter"
                loading={dashboardLoading}
                highlighted={crossFilter?.type==='purch_group'} onClear={clearCF}>
                <EmptyState condition={!dashboardLoading && purchGroupWorkload.length === 0} message="No workload data found.">
                  <PurchGroupWorkloadChart data={purchGroupWorkload} crossFilter={crossFilter} onSelect={handleSelect} />
                </EmptyState>
              </ChartCard>

              <ChartCard title="Avg Days PO → GR by Vendor"
                loading={dashboardLoading}
                highlighted={crossFilter?.type==='vendor'} onClear={clearCF}>
                <EmptyState condition={!dashboardLoading && vendorLeadTime.length === 0} message="No GR data found to calculate lead time.">
                  <VendorAvgDaysChart data={vendorLeadTime} crossFilter={crossFilter} onSelect={handleSelect} />
                </EmptyState>
              </ChartCard>
            </div>
          )}

        </>)}

        {/* --- NEW FILE HUB UI INSTEAD OF "NO DATA LOADED" --- */}
              </div>

      <div style={{ textAlign: 'center', fontSize: '12px', color: '#605E5C', padding: '10px 0', borderTop: '1px solid #E1DFDD', flexShrink: 0, zIndex: 100 }}>
        ©2023 <a href="https://ajalabs.ai" target="_blank" rel="noopener noreferrer" style={{ color: '#323130', textDecoration: 'none', fontWeight: 'bold' }}>ajalabs.ai</a> All rights reserved - <a href="#" style={{ color: '#0078D4', textDecoration: 'none' }}>Data Privacy</a>
      </div>

    </div>
  );
}

//comment
