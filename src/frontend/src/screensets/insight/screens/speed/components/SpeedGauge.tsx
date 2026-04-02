/**
 * SpeedGauge Component
 * Half-donut speedometer gauge using PieChart from recharts via @hai3/uikit
 */

import React from 'react';
import {
  PieChart,
  Pie,
  Cell,
  ResponsiveContainer,
} from '@hai3/uikit';
import type { SpeedData } from '../../../types';

interface SpeedGaugeProps {
  data: SpeedData;
}

const RADIAN = Math.PI / 180;

/**
 * Renders a needle on the gauge at the calculated angle
 */
const renderNeedle = (
  value: number,
  min: number,
  max: number,
  cx: number,
  cy: number,
  innerRadius: number,
  outerRadius: number,
): React.ReactElement => {
  const range = max - min;
  const clampedValue = Math.max(min, Math.min(max, value));
  const angle = 180 - ((clampedValue - min) / range) * 180;
  const needleLength = (innerRadius + outerRadius) / 2;
  const needleBaseWidth = 6;

  const tipX = cx + needleLength * Math.cos(angle * RADIAN);
  const tipY = cy - needleLength * Math.sin(angle * RADIAN);

  const leftX = cx + needleBaseWidth * Math.cos((angle - 90) * RADIAN);
  const leftY = cy - needleBaseWidth * Math.sin((angle - 90) * RADIAN);
  const rightX = cx + needleBaseWidth * Math.cos((angle + 90) * RADIAN);
  const rightY = cy - needleBaseWidth * Math.sin((angle + 90) * RADIAN);

  return (
    <g>
      <polygon
        points={`${tipX},${tipY} ${leftX},${leftY} ${rightX},${rightY}`}
        className="fill-primary"
      />
      <circle cx={cx} cy={cy} r={8} className="fill-primary" />
      <circle cx={cx} cy={cy} r={4} className="fill-background" />
    </g>
  );
};

/**
 * Generate tick marks for the gauge
 */
const renderTicks = (
  min: number,
  max: number,
  cx: number,
  cy: number,
  outerRadius: number,
  tickCount: number,
): React.ReactElement[] => {
  const ticks: React.ReactElement[] = [];
  const range = max - min;

  for (let i = 0; i <= tickCount; i++) {
    const tickValue = min + (range / tickCount) * i;
    const angle = 180 - (i / tickCount) * 180;
    const isMajor = i % 2 === 0;
    const tickInner = outerRadius + (isMajor ? 8 : 12);
    const tickOuter = outerRadius + 20;
    const labelRadius = outerRadius + 32;

    const x1 = cx + tickInner * Math.cos(angle * RADIAN);
    const y1 = cy - tickInner * Math.sin(angle * RADIAN);
    const x2 = cx + tickOuter * Math.cos(angle * RADIAN);
    const y2 = cy - tickOuter * Math.sin(angle * RADIAN);
    const labelX = cx + labelRadius * Math.cos(angle * RADIAN);
    const labelY = cy - labelRadius * Math.sin(angle * RADIAN);

    ticks.push(
      <line
        key={`tick-${i}`}
        x1={x1}
        y1={y1}
        x2={x2}
        y2={y2}
        className={isMajor ? 'stroke-foreground' : 'stroke-muted-foreground'}
        strokeWidth={isMajor ? 2 : 1}
      />
    );

    if (isMajor) {
      ticks.push(
        <text
          key={`label-${i}`}
          x={labelX}
          y={labelY}
          textAnchor="middle"
          dominantBaseline="middle"
          className="fill-foreground text-xs font-medium"
          fontSize={12}
        >
          {Math.round(tickValue)}
        </text>
      );
    }
  }

  return ticks;
};

export const SpeedGauge: React.FC<SpeedGaugeProps> = ({ data }) => {
  const { value, min, max, unit } = data;

  // Create gauge segments for the half-donut
  const segments = 20;
  const segmentSize = (max - min) / segments;
  const gaugeData = Array.from({ length: segments }, (_, i) => ({
    value: segmentSize,
    index: i,
  }));

  // Background (remaining half) to make it a semicircle
  const backgroundData = [{ value: max - min }];

  const cx = 200;
  const cy = 180;
  const innerRadius = 100;
  const outerRadius = 140;

  return (
    <div className="flex flex-col items-center">
      <div className="w-[400px] h-[240px]">
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            {/* Background arc */}
            <Pie
              data={backgroundData}
              cx={cx}
              cy={cy}
              startAngle={180}
              endAngle={0}
              innerRadius={innerRadius}
              outerRadius={outerRadius}
              dataKey="value"
              stroke="none"
              isAnimationActive={false}
            >
              <Cell className="fill-muted" />
            </Pie>

            {/* Colored segments */}
            <Pie
              data={gaugeData}
              cx={cx}
              cy={cy}
              startAngle={180}
              endAngle={0}
              innerRadius={innerRadius}
              outerRadius={outerRadius}
              dataKey="value"
              stroke="none"
              isAnimationActive={false}
            >
              {gaugeData.map((entry) => {
                const segmentMidValue = min + (entry.index + 0.5) * segmentSize;
                const isActive = segmentMidValue <= value;
                const ratio = segmentMidValue / max;

                let colorClass = 'fill-emerald-500';
                if (ratio > 0.8) colorClass = 'fill-red-500';
                else if (ratio > 0.6) colorClass = 'fill-orange-500';
                else if (ratio > 0.4) colorClass = 'fill-yellow-500';

                return (
                  <Cell
                    key={`segment-${entry.index}`}
                    className={isActive ? colorClass : 'fill-transparent'}
                  />
                );
              })}
            </Pie>

            {/* Tick marks and labels */}
            {renderTicks(min, max, cx, cy, outerRadius, 14)}

            {/* Needle */}
            {renderNeedle(value, min, max, cx, cy, innerRadius, outerRadius)}

            {/* Center value text */}
            <text
              x={cx}
              y={cy - 20}
              textAnchor="middle"
              className="fill-foreground font-bold"
              fontSize={36}
            >
              {value}
            </text>
            <text
              x={cx}
              y={cy + 5}
              textAnchor="middle"
              className="fill-muted-foreground"
              fontSize={14}
            >
              {unit}
            </text>
          </PieChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

SpeedGauge.displayName = 'SpeedGauge';
